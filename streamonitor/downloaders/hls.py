import m3u8
import os
import shutil
import subprocess
from threading import Thread
from time import monotonic, sleep
from urllib.parse import urljoin

from ffmpy import FFmpeg, FFRuntimeError

from parameters import DEBUG, CONTAINER, SEGMENT_TIME, FFMPEG_PATH

_http_lib = None
if not _http_lib:
    try:
        import pycurl_requests as requests
        _http_lib = 'pycurl'
    except ImportError:
        pass
if not _http_lib:
    try:
        import requests
        _http_lib = 'requests'
    except ImportError:
        pass
if not _http_lib:
    raise ImportError("Please install requests or pycurl package to proceed")


def _create_download_session(self):
    session = requests.Session()
    if hasattr(self, 'headers') and isinstance(self.headers, dict):
        session.headers.update(self.headers)

    source_session = getattr(self, 'session', None)
    source_cookies = getattr(source_session, 'cookies', None)
    if source_cookies is not None:
        try:
            session.cookies.update(source_cookies)
        except Exception:
            pass

    extra_cookies = getattr(self, 'cookies', None)
    if extra_cookies is not None:
        try:
            session.cookies.update(extra_cookies)
        except Exception:
            pass

    return session


def _get_filename_suffix(self):
    if hasattr(self, 'filename_extra_suffix'):
        return self.filename_extra_suffix
    return ''


def _build_output_target(self, filename):
    basefilename = filename[:-len('.' + CONTAINER)]
    suffix = _get_filename_suffix(self)
    output_target = basefilename + suffix + '.' + CONTAINER
    output_str = '-c:a copy -c:v copy'
    if SEGMENT_TIME is not None:
        output_str += f' -f segment -reset_timestamps 1 -segment_time {str(SEGMENT_TIME)}'
        output_target = basefilename + '_%03d' + suffix + '.' + CONTAINER
    return basefilename, output_target, output_str


def _run_ffmpeg(input_path, input_options, output_path, output_options, stdout, stderr):
    ff = FFmpeg(
        executable=FFMPEG_PATH,
        inputs={input_path: input_options},
        outputs={output_path: output_options}
    )
    ff.run(stdout=stdout, stderr=stderr)


def _finalize_recording(self, input_path, filename, input_options=None):
    _, output_target, output_str = _build_output_target(self, filename)
    stdout = open(filename + '.postprocess_stdout.log', 'w+') if DEBUG else subprocess.DEVNULL
    stderr = open(filename + '.postprocess_stderr.log', 'w+') if DEBUG else subprocess.DEVNULL
    try:
        _run_ffmpeg(input_path, input_options, output_target, output_str, stdout, stderr)
    finally:
        if stdout not in (None, subprocess.DEVNULL):
            stdout.close()
        if stderr not in (None, subprocess.DEVNULL):
            stderr.close()


def _cleanup_paths(paths):
    for path in paths:
        if not path:
            continue
        if os.path.isdir(path):
            shutil.rmtree(path, ignore_errors=True)
        elif os.path.exists(path):
            try:
                os.remove(path)
            except OSError:
                pass


def _concat_recordings(self, part_files, filename):
    if len(part_files) == 1:
        _finalize_recording(self, part_files[0], filename)
        return True

    basefilename, _, _ = _build_output_target(self, filename)
    parts_dir = os.path.dirname(part_files[0])
    concat_list = os.path.join(parts_dir, 'concat.txt')
    merged_tmpfilename = basefilename + '.merged.mp4'

    with open(concat_list, 'w', encoding='utf-8') as concat_file:
        for part_file in part_files:
            escaped_part_file = part_file.replace("'", "'\\''")
            concat_file.write(f"file '{escaped_part_file}'\n")

    stdout = open(filename + '.postprocess_stdout.log', 'w+') if DEBUG else subprocess.DEVNULL
    stderr = open(filename + '.postprocess_stderr.log', 'w+') if DEBUG else subprocess.DEVNULL
    try:
        try:
            _run_ffmpeg(
                concat_list,
                '-f concat -safe 0',
                merged_tmpfilename,
                '-c:a copy -c:v copy',
                stdout,
                stderr
            )
        except FFRuntimeError as e:
            if e.exit_code and e.exit_code != 255:
                self.logger.warning('Concat copy failed after adaptive quality switch, retrying with re-encode')
                if os.path.exists(merged_tmpfilename):
                    os.remove(merged_tmpfilename)
                _run_ffmpeg(
                    concat_list,
                    '-f concat -safe 0',
                    merged_tmpfilename,
                    '-c:v libx264 -preset veryfast -crf 20 -c:a aac -b:a 160k',
                    stdout,
                    stderr
                )
            else:
                raise
    finally:
        if stdout not in (None, subprocess.DEVNULL):
            stdout.close()
        if stderr not in (None, subprocess.DEVNULL):
            stderr.close()

    try:
        _finalize_recording(self, merged_tmpfilename, filename)
    finally:
        _cleanup_paths([concat_list, merged_tmpfilename])
    return True


def _segment_uri_is_fmp4(segment):
    return segment.uri.endswith(('.m4s', '.mp4', '.cmfv', '.cmfa'))


def _resolve_chunk_uri(playlist_url, chunk_uri):
    return urljoin(playlist_url, chunk_uri)


def getVideoNativeHLS(self, url, filename, m3u_processor=None):
    self.stopDownloadFlag = False
    error = False
    session = _create_download_session(self)

    def execute():
        nonlocal error
        downloaded_list = []
        outfile = None
        tmpfilename = None
        try:
            while not self.stopDownloadFlag:
                downloaded_in_iteration = False
                r = session.get(url, headers=self.headers, cookies=self.cookies)
                if r.status_code != 200:
                    self.logger.warning(f'Playlist request failed with HTTP {r.status_code}: {url}')
                    return
                content = r.content.decode("utf-8")
                if m3u_processor:
                    processed_content = m3u_processor(content)
                    if processed_content is not None:
                        content = processed_content
                chunklist = m3u8.loads(content)
                if len(chunklist.segments) == 0:
                    self.logger.warning(f'Playlist returned no media segments: {url}')
                    return

                if outfile is None:
                    uses_fmp4 = len(chunklist.segment_map) > 0 or any(
                        _segment_uri_is_fmp4(chunk)
                        for chunk in chunklist.segments
                    )
                    tmp_extension = '.tmp.mp4' if uses_fmp4 else '.tmp.ts'
                    tmpfilename = filename[:-len('.' + CONTAINER)] + tmp_extension
                    outfile = open(tmpfilename, 'wb')

                for chunk in chunklist.segment_map + chunklist.segments:
                    if chunk.uri in downloaded_list:
                        continue
                    downloaded_in_iteration = True
                    downloaded_list.append(chunk.uri)
                    chunk_uri = _resolve_chunk_uri(url, chunk.uri)
                    self.debug('Downloading ' + chunk_uri)
                    m = session.get(chunk_uri, headers=self.headers, cookies=self.cookies)
                    if m.status_code != 200:
                        self.logger.warning(f'Media segment request failed with HTTP {m.status_code}: {chunk_uri}')
                        return
                    outfile.write(m.content)
                    if self.stopDownloadFlag:
                        return

                if not downloaded_in_iteration:
                    sleep(2)
        except Exception:
            error = True
            raise
        finally:
            if outfile is not None:
                outfile.close()

    def terminate():
        self.stopDownloadFlag = True

    process = Thread(target=execute)
    process.start()
    self.stopDownload = terminate
    process.join()
    self.stopDownload = None

    if error:
        return False

    tmpfilename = filename[:-len('.' + CONTAINER)] + '.tmp.mp4'
    if not os.path.exists(tmpfilename):
        tmpfilename = filename[:-len('.' + CONTAINER)] + '.tmp.ts'
    if not os.path.exists(tmpfilename):
        return False

    if os.path.getsize(tmpfilename) == 0:
        os.remove(tmpfilename)
        return False

    try:
        _finalize_recording(self, tmpfilename, filename)
    except FFRuntimeError as e:
        if e.exit_code and e.exit_code != 255:
            return False
    finally:
        if os.path.exists(tmpfilename):
            os.remove(tmpfilename)

    return True


def getVideoAdaptiveHLS(self, url, filename, m3u_processor=None, variant_selector=None, switch_check_interval=15):
    self.stopDownloadFlag = False
    error = False
    session = _create_download_session(self)
    basefilename = filename[:-len('.' + CONTAINER)]
    parts_dir = basefilename + '.parts'
    os.makedirs(parts_dir, exist_ok=True)

    current_variant_url = url
    current_variant_info = None
    current_part_handle = None
    current_part_path = None
    current_part_index = 0
    current_part_init_segments = set()
    part_files = []
    downloaded_media_segments = set()
    last_switch_check = 0.0

    def describe_variant(source):
        resolution = source.get('resolution') or (0, 0)
        codecs = source.get('codecs') or 'unknown codec'
        stream_type = 'fMP4' if source.get('is_fmp4') else 'HLS'
        return f'{resolution[0]}x{resolution[1]} [{codecs}] ({stream_type})'

    def open_part(chunklist):
        nonlocal current_part_handle, current_part_path, current_part_init_segments, current_part_index
        uses_fmp4 = len(chunklist.segment_map) > 0 or any(
            _segment_uri_is_fmp4(chunk)
            for chunk in chunklist.segments
        )
        extension = '.mp4' if uses_fmp4 else '.ts'
        current_part_path = os.path.join(parts_dir, f'part_{current_part_index:04d}{extension}')
        current_part_handle = open(current_part_path, 'wb')
        current_part_init_segments = set()
        part_files.append(current_part_path)

    def close_part():
        nonlocal current_part_handle, current_part_path
        if current_part_handle is not None:
            current_part_handle.close()
        current_part_handle = None
        current_part_path = None

    def execute():
        nonlocal error, current_variant_url, current_variant_info, last_switch_check, current_part_index
        try:
            while not self.stopDownloadFlag:
                now = monotonic()
                source_session = getattr(self, 'session', None)
                source_cookies = getattr(source_session, 'cookies', None)
                if source_cookies is not None:
                    try:
                        session.cookies.update(source_cookies)
                    except Exception:
                        pass
                if variant_selector is not None and (current_variant_info is None or now - last_switch_check >= switch_check_interval):
                    candidate_variant = variant_selector()
                    last_switch_check = now
                    if candidate_variant is not None:
                        if current_variant_info is None:
                            current_variant_info = candidate_variant
                            current_variant_url = candidate_variant['url']
                        elif candidate_variant['url'] != current_variant_url:
                            self.log(f'Switching stream variant to {describe_variant(candidate_variant)}')
                            close_part()
                            current_part_index += 1
                            current_variant_info = candidate_variant
                            current_variant_url = candidate_variant['url']
                        else:
                            current_variant_info = candidate_variant

                r = session.get(current_variant_url, headers=self.headers, cookies=self.cookies)
                if r.status_code != 200:
                    self.logger.warning(f'Playlist request failed with HTTP {r.status_code}: {current_variant_url}')
                    return
                content = r.content.decode("utf-8")
                if m3u_processor:
                    processed_content = m3u_processor(content)
                    if processed_content is not None:
                        content = processed_content
                chunklist = m3u8.loads(content)
                if len(chunklist.segments) == 0:
                    self.logger.warning(f'Playlist returned no media segments: {current_variant_url}')
                    return

                if current_part_handle is None:
                    open_part(chunklist)

                downloaded_in_iteration = False
                for chunk in chunklist.segment_map + chunklist.segments:
                    chunk_url = _resolve_chunk_uri(current_variant_url, chunk.uri)
                    if chunk in chunklist.segment_map:
                        if chunk_url in current_part_init_segments:
                            continue
                        current_part_init_segments.add(chunk_url)
                    else:
                        if chunk_url in downloaded_media_segments:
                            continue
                        downloaded_media_segments.add(chunk_url)

                    downloaded_in_iteration = True
                    self.debug('Downloading ' + chunk_url)
                    m = session.get(chunk_url, headers=self.headers, cookies=self.cookies)
                    if m.status_code != 200:
                        self.logger.warning(f'Media segment request failed with HTTP {m.status_code}: {chunk_url}')
                        return
                    current_part_handle.write(m.content)
                    if self.stopDownloadFlag:
                        return

                if not downloaded_in_iteration:
                    sleep(2)
        except Exception:
            error = True
            raise
        finally:
            close_part()

    def terminate():
        self.stopDownloadFlag = True

    process = Thread(target=execute)
    process.start()
    self.stopDownload = terminate
    process.join()
    self.stopDownload = None

    if error or not part_files:
        _cleanup_paths([parts_dir])
        return False

    for part_file in list(part_files):
        if not os.path.exists(part_file) or os.path.getsize(part_file) == 0:
            part_files.remove(part_file)
            _cleanup_paths([part_file])

    if not part_files:
        _cleanup_paths([parts_dir])
        return False

    try:
        _concat_recordings(self, part_files, filename)
    except FFRuntimeError as e:
        if e.exit_code and e.exit_code != 255:
            return False
    finally:
        _cleanup_paths(part_files + [parts_dir])

    return True
