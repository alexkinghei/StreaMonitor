import m3u8
import os
import re
import subprocess
import time
from threading import Thread
from ffmpy import FFmpeg, FFRuntimeError
from time import sleep
from parameters import (
    DEBUG, CONTAINER, SEGMENT_SIZE, parse_segment_size, FFMPEG_PATH,
    HLS_TRANSIENT_GRACE_SECONDS, HLS_RETRY_SLEEP_SECONDS,
)

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

def _rename_mp4_by_title(mp4_path, logger=None):
    """If base.title.txt exists with non-empty title, rename mp4 to base-title.mp4 and remove .title.txt."""
    if not mp4_path or not mp4_path.endswith('.' + CONTAINER):
        return
    folder = os.path.dirname(mp4_path)
    base = os.path.basename(mp4_path)[: -len('.' + CONTAINER)]
    title_path = os.path.join(folder, base + '.title.txt')
    if not os.path.exists(title_path):
        return
    try:
        with open(title_path, 'r', encoding='utf-8') as f:
            title = (f.read() or '').strip()
    except OSError:
        return
    if not title:
        try:
            os.remove(title_path)
        except OSError:
            pass
        return
    # Only replace filesystem-illegal chars (keep spaces and original punctuation like ~ !)
    illegal_fs = r'[<>:"/\\|?*\x00-\x1f]'
    safe_title = re.sub(illegal_fs, '_', title)
    safe_title = re.sub(r'\s+', ' ', safe_title)  # collapse spaces to single space, keep space
    safe_title = re.sub(r'_+', '_', safe_title).strip(' ._')
    # Allow longer title in filename (200 bytes) so long titles are not cut off
    if len(safe_title.encode('utf-8')) > 200:
        safe_title = safe_title.encode('utf-8')[:200].decode('utf-8', errors='ignore').strip(' ._')
    if not safe_title:
        try:
            os.remove(title_path)
        except OSError:
            pass
        return
    final_path = os.path.join(folder, base + '-' + safe_title + '.' + CONTAINER)
    if final_path == mp4_path or not os.path.exists(mp4_path):
        return
    try:
        os.rename(mp4_path, final_path)
    except OSError as e:
        if logger:
            logger.warning('Could not rename to title filename: %s', e)
        return
    # Delete .title.txt only after successful rename
    try:
        os.remove(title_path)
    except OSError as e:
        if logger:
            logger.warning('Could not remove .title.txt after rename: %s', e)


def getVideoNativeHLS(self, url, filename, m3u_processor=None):
    self.stopDownloadFlag = False
    error = False
    tmpfilename = filename[:-len('.' + CONTAINER)] + '.tmp.ts'
    session = requests.Session()
    # Mutable so we can refresh playlist URL when stream changes bitrate/variants
    url_ref = [url]

    # Check if we should segment during download
    segment_size_bytes = parse_segment_size(SEGMENT_SIZE)
    segment_during_download = segment_size_bytes is not None
    current_file = None
    current_file_size = 0
    segment_files = []  # Track all segment files for final conversion
    segment_convert_threads = []  # Background conversion threads to join before return
    successful_outputs = [0]
    # fMP4 streams: cache init segment (ftyp+moov) so we can prepend it to new files and recovery remuxes.
    init_segment_bytes = [b'']

    def _looks_like_mp4_fragment_bytes(data: bytes) -> bool:
        if len(data) < 8:
            return False
        box_type = data[4:8]
        return box_type in {b'ftyp', b'moov', b'moof', b'mdat', b'styp', b'sidx', b'free', b'uuid'}

    def _looks_like_mpegts_bytes(data: bytes) -> bool:
        if len(data) < 188:
            return False
        # Accept the common TS packet sizes (188/192/204) and allow offset starts.
        for packet_size in (188, 192, 204):
            max_offset = min(packet_size, 32)
            for offset in range(max_offset):
                if offset + (packet_size * 2) >= len(data):
                    continue
                if (
                    data[offset] == 0x47 and
                    data[offset + packet_size] == 0x47 and
                    data[offset + (packet_size * 2)] == 0x47
                ):
                    return True
        return False

    def _looks_like_mp4_with_header(path: str) -> bool:
        try:
            with open(path, 'rb') as f:
                header = f.read(16)
            return _looks_like_mp4_fragment_bytes(header)
        except OSError:
            return False

    def _looks_like_mpegts(path: str) -> bool:
        try:
            with open(path, 'rb') as f:
                header = f.read(4096)
            return _looks_like_mpegts_bytes(header)
        except OSError:
            return False

    def _mark_problematic_file(path: str, reason: str):
        marker_path = path + '.invalid.txt'
        ts_now = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        try:
            with open(marker_path, 'a', encoding='utf-8') as f:
                f.write(f'[{ts_now}] {reason}\n')
        except OSError:
            pass

    def _make_input_with_init_if_needed(input_path: str):
        """
        Return (input_for_ffmpeg, temp_path_or_none).
        If file appears to miss MP4 init and we have cached init bytes, prepend init into a temp file.
        """
        if not init_segment_bytes[0]:
            return input_path, None
        if _looks_like_mp4_with_header(input_path) or _looks_like_mpegts(input_path):
            return input_path, None
        tmp_with_init = input_path + '.with_init.tmp.ts'
        try:
            with open(tmp_with_init, 'wb') as out_f:
                out_f.write(init_segment_bytes[0])
                with open(input_path, 'rb') as in_f:
                    while True:
                        block = in_f.read(1024 * 1024)
                        if not block:
                            break
                        out_f.write(block)
            return tmp_with_init, tmp_with_init
        except OSError:
            try:
                if os.path.exists(tmp_with_init):
                    os.remove(tmp_with_init)
            except OSError:
                pass
            return input_path, None

    missing_init_helper_warned = [False]

    def _safe_make_input_with_init_if_needed(input_path: str):
        """
        Defensive wrapper: even if helper lookup fails unexpectedly at runtime,
        keep conversion running with the raw input instead of aborting recording.
        """
        try:
            helper = _make_input_with_init_if_needed
        except Exception as e:
            if not missing_init_helper_warned[0]:
                self.logger.warning(
                    "Init-header helper unavailable at runtime; fallback to raw input for conversion (%s)",
                    e,
                )
                missing_init_helper_warned[0] = True
            return input_path, None
        try:
            return helper(input_path)
        except Exception as e:
            if not missing_init_helper_warned[0]:
                self.logger.warning(
                    "Init-header helper failed; fallback to raw input for conversion (%s)",
                    e,
                )
                missing_init_helper_warned[0] = True
            return input_path, None

    def _iter_input_demuxers(input_path: str):
        """
        Yield ffmpeg input demuxers to try in a stable order:
        auto-detect first, then detected/known demuxers.
        """
        detected = None
        if _looks_like_mpegts(input_path):
            detected = 'mpegts'
        elif _looks_like_mp4_with_header(input_path):
            detected = 'mp4'
        seen = set()
        for demuxer in (None, detected, 'mpegts', 'mp4'):
            if demuxer in seen:
                continue
            seen.add(demuxer)
            yield demuxer

    def _iter_ffprobe_bins():
        ffmpeg_bin = os.path.basename(FFMPEG_PATH)
        ffmpeg_dir = os.path.dirname(FFMPEG_PATH)
        candidates = []
        if ffmpeg_bin.startswith('ffmpeg'):
            probe_bin = ffmpeg_bin.replace('ffmpeg', 'ffprobe', 1)
            if ffmpeg_dir:
                candidates.append(os.path.join(ffmpeg_dir, probe_bin))
            else:
                candidates.append(probe_bin)
        candidates.append('ffprobe')
        seen = set()
        for c in candidates:
            if c in seen:
                continue
            seen.add(c)
            yield c

    def _probe_has_av_streams(path: str):
        """
        Return:
        - True: at least one audio/video stream
        - False: no streams or probe failed
        - None: ffprobe not available
        """
        for ffprobe_bin in _iter_ffprobe_bins():
            try:
                res = subprocess.run(
                    [
                        ffprobe_bin,
                        '-v', 'error',
                        '-show_entries', 'stream=codec_type',
                        '-of', 'default=noprint_wrappers=1:nokey=1',
                        path,
                    ],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.DEVNULL,
                    text=True,
                    check=False,
                )
            except OSError:
                continue
            if res.returncode != 0:
                return False
            for line in res.stdout.splitlines():
                if line.strip().lower() in {'audio', 'video'}:
                    return True
            return False
        return None

    def _run_recovery_remux(input_path: str, output_path: str, stderr_target) -> bool:
        """
        Retry remux with corruption-tolerant flags and multiple input demuxers.
        This recovers cases where ffmpeg mis-detects .ts/fMP4 fragments by extension/content.
        """
        has_streams = _probe_has_av_streams(input_path)
        if has_streams is False:
            return False
        for demuxer in _iter_input_demuxers(input_path):
            cmd = [
                FFMPEG_PATH, '-y',
                '-err_detect', 'ignore_err',
                '-fflags', '+discardcorrupt+genpts',
            ]
            if demuxer:
                cmd.extend(['-f', demuxer])
            cmd.extend([
                '-i', input_path,
                '-c:a', 'copy',
                '-c:v', 'copy',
                output_path,
            ])
            try:
                subprocess.run(
                    cmd,
                    stdout=subprocess.DEVNULL,
                    stderr=stderr_target,
                    check=True,
                )
                return True
            except Exception:
                try:
                    if os.path.exists(output_path):
                        os.remove(output_path)
                except OSError:
                    pass
        return False

    if segment_during_download:
        # Use the initial filename for first segment (but as .ts)
        current_file = filename[:-len('.' + CONTAINER)] + '.ts'
        current_file_size = 0

    def execute():
        nonlocal error, current_file, current_file_size, segment_files, segment_convert_threads
        downloaded_set = set()
        outfile = [None]  # Use list to allow modification in nested function
        last_success = time.monotonic()
        current_file_has_init = [False]
        current_file_has_media = [False]
        just_rotated = [False]  # True only after 800MB rotation so we prepend init only to new segment files

        def within_grace():
            return (time.monotonic() - last_success) <= float(HLS_TRANSIENT_GRACE_SECONDS)

        def init_chunk_key(uri: str) -> str:
            # Strip query/fragment so rotating auth tokens don't make the same init look "new".
            return uri.split('?', 1)[0].split('#', 1)[0]

        def try_refresh_url():
            """On bitrate/playlist change, get fresh playlist URL and continue same recording."""
            try:
                new_url = self.getVideoUrl()
                if new_url:
                    url_ref[0] = new_url
                    downloaded_set.clear()
                    self.logger.info('Refreshed playlist URL (stream may have changed bitrate/variants)')
                    return True
            except Exception as e:
                self.logger.debug('Refresh playlist URL failed: %s', e)
            return False
        
        def convert_segment_to_mp4(ts_file_path, final_filename):
            """Convert a .ts segment file to final format (MP4) in background thread"""
            def convert():
                stderr_path = final_filename + '.postprocess_stderr.log'
                stderr_file = None
                input_tmp_path = None
                try:
                    # Wait a bit to ensure file is fully written and closed (fsync in main thread helps)
                    sleep(0.5)
                    
                    if not os.path.exists(ts_file_path):
                        return
                    sz = os.path.getsize(ts_file_path)
                    if sz == 0:
                        try:
                            os.remove(ts_file_path)
                        except OSError:
                            pass
                        return
                    
                    ffmpeg_input_path, input_tmp_path = _safe_make_input_with_init_if_needed(ts_file_path)
                    has_streams = _probe_has_av_streams(ffmpeg_input_path)
                    if has_streams is False:
                        if input_tmp_path:
                            try:
                                os.remove(input_tmp_path)
                            except OSError:
                                pass
                        _mark_problematic_file(
                            ts_file_path,
                            'ffprobe detected no audio/video streams during segment conversion',
                        )
                        self.logger.warning('Segment has no streams; kept for inspection: %s', ts_file_path)
                        return
                    stdout = open(final_filename + '.postprocess_stdout.log', 'w+') if DEBUG else subprocess.DEVNULL
                    stderr_file = open(stderr_path, 'w+')
                    output_str = '-c:a copy -c:v copy'
                    ff = FFmpeg(executable=FFMPEG_PATH, inputs={ffmpeg_input_path: None}, outputs={final_filename: output_str})
                    ff.run(stdout=stdout, stderr=stderr_file)
                    stderr_file.close()
                    stderr_file = None
                    try:
                        os.remove(stderr_path)
                    except OSError:
                        pass
                    if input_tmp_path:
                        try:
                            os.remove(input_tmp_path)
                        except OSError:
                            pass
                    os.remove(ts_file_path)
                    _rename_mp4_by_title(final_filename, self.logger)
                    successful_outputs[0] += 1
                except FFRuntimeError as e:
                    if stderr_file:
                        try:
                            stderr_file.close()
                        except OSError:
                            pass
                    try:
                        if os.path.exists(final_filename):
                            os.remove(final_filename)
                    except OSError:
                        pass
                    ffmpeg_input_path, input_tmp_path = _safe_make_input_with_init_if_needed(ts_file_path)
                    # Retry once with corruption-tolerant demux flags to salvage partial segments.
                    try:
                        with open(stderr_path, 'a+') as fallback_stderr:
                            recovered = _run_recovery_remux(
                                ffmpeg_input_path,
                                final_filename,
                                fallback_stderr,
                            )
                        if not recovered:
                            raise RuntimeError("Recovery remux failed")
                        try:
                            os.remove(stderr_path)
                        except OSError:
                            pass
                        if input_tmp_path:
                            try:
                                os.remove(input_tmp_path)
                            except OSError:
                                pass
                        os.remove(ts_file_path)
                        _rename_mp4_by_title(final_filename, self.logger)
                        successful_outputs[0] += 1
                        return
                    except Exception:
                        pass
                    if input_tmp_path:
                        try:
                            os.remove(input_tmp_path)
                        except OSError:
                            pass
                    # Log all non-zero exit codes (255 was previously ignored and caused silent failures)
                    if e.exit_code:
                        self.logger.error(
                            'Error converting segment (exit_code=%s): %s. Check %r for ffmpeg stderr.',
                            e.exit_code, e, stderr_path
                        )
                    _mark_problematic_file(
                        ts_file_path,
                        f'ffmpeg segment remux failed (exit_code={getattr(e, "exit_code", "unknown")})',
                    )
                    # Remove partial/corrupt mp4 if ffmpeg left one
                    try:
                        if os.path.exists(final_filename):
                            os.remove(final_filename)
                    except OSError:
                        pass
                except Exception as e:
                    if stderr_file:
                        try:
                            stderr_file.close()
                        except OSError:
                            pass
                    if input_tmp_path:
                        try:
                            os.remove(input_tmp_path)
                        except OSError:
                            pass
                    self.logger.error(f'Unexpected error converting segment: {e}. Check {stderr_path!r} for ffmpeg stderr.')
                    try:
                        if os.path.exists(final_filename):
                            os.remove(final_filename)
                    except OSError:
                        pass
            
            # Run conversion in background thread (like pause/resume does)
            convert_thread = Thread(target=convert)
            convert_thread.start()
            return convert_thread
        
        def get_output_file():
            nonlocal current_file, current_file_size, segment_files, segment_convert_threads
            if segment_during_download:
                # Check if we need to start a new segment
                if current_file_size >= int(segment_size_bytes):
                    prev_has_media = current_file_has_media[0]
                    if outfile[0] and not outfile[0].closed:
                        outfile[0].flush()
                        try:
                            os.fsync(outfile[0].fileno())
                        except (OSError, AttributeError):
                            pass
                        outfile[0].close()

                    # Convert the completed segment to MP4 (like pause/resume logic)
                    prev_ts_file = current_file
                    # Generate final filename by replacing .ts with .mp4
                    prev_final_filename = prev_ts_file.replace('.ts', '.' + CONTAINER)
                    if os.path.exists(prev_ts_file):
                        prev_sz = os.path.getsize(prev_ts_file)
                        if prev_sz > 0:
                            if prev_has_media:
                                segment_files.append((prev_ts_file, prev_final_filename))
                                t = convert_segment_to_mp4(prev_ts_file, prev_final_filename)
                                if t is not None:
                                    segment_convert_threads.append(t)
                            else:
                                _mark_problematic_file(
                                    prev_ts_file,
                                    'segment rotated without media payload (init-only or invalid chunks)',
                                )
                                self.logger.warning('Segment kept for inspection (no media payload): %s', prev_ts_file)
                        else:
                            try:
                                os.remove(prev_ts_file)
                            except OSError:
                                pass

                    # Generate new filename with timestamp (like pause/resume)
                    new_filename = self.genOutFilename(create_dir=True)
                    current_file = new_filename[:-len('.' + CONTAINER)] + '.ts'
                    current_file_size = 0
                    current_file_has_init[0] = False
                    current_file_has_media[0] = False
                    just_rotated[0] = True
                if outfile[0] is None or outfile[0].closed:
                    # Use wb so a rare filename collision never appends stale bytes from an older file.
                    outfile[0] = open(current_file, 'wb')
                    # fMP4: prepend init segment only after rotation (first file gets init from the loop)
                    if init_segment_bytes[0] and just_rotated[0]:
                        outfile[0].write(init_segment_bytes[0])
                        current_file_size += len(init_segment_bytes[0])
                        current_file_has_init[0] = True
                        just_rotated[0] = False
                return outfile[0]
            else:
                if outfile[0] is None:
                    outfile[0] = open(tmpfilename, 'wb')
                return outfile[0]
        
        try:
            while not self.stopDownloadFlag:
                did_download = False
                try:
                    r = session.get(url_ref[0], headers=self.headers, cookies=self.cookies, timeout=30)
                except Exception as e:
                    if within_grace():
                        if try_refresh_url():
                            sleep(float(HLS_RETRY_SLEEP_SECONDS))
                        else:
                            self.logger.warning('HLS playlist fetch failed (transient, retrying): %s', e)
                            sleep(float(HLS_RETRY_SLEEP_SECONDS))
                        continue
                    self.logger.warning('HLS playlist fetch failed (giving up): %s', e)
                    error = True
                    return
                content = r.content.decode("utf-8")
                if m3u_processor:
                    processed = m3u_processor(content)
                    if isinstance(processed, str) and processed:
                        content = processed
                chunklist = m3u8.loads(content)
                if len(chunklist.segments) == 0:
                    # Sometimes live playlists temporarily return empty; tolerate within grace.
                    if within_grace():
                        if try_refresh_url():
                            pass
                        sleep(float(HLS_RETRY_SLEEP_SECONDS))
                        continue
                    return
                # Support both MPEG-TS and fMP4: segment_map is the init segment(s), segments are media.
                _sm = getattr(chunklist, 'segment_map', None)
                init_list = _sm if isinstance(_sm, list) else ([_sm] if _sm else [])
                combined = init_list + list(chunklist.segments)
                n_init = len(init_list)
                playlist_init_parts = [None] * n_init if segment_during_download and n_init > 0 else None
                for i, chunk in enumerate(combined):
                    is_init_chunk = i < n_init
                    chunk_key = init_chunk_key(chunk.uri) if is_init_chunk else chunk.uri
                    if chunk_key in downloaded_set and (not is_init_chunk or current_file_has_init[0]):
                        continue
                    did_download = True
                    chunk_key_was_new = chunk_key not in downloaded_set
                    if chunk_key_was_new:
                        downloaded_set.add(chunk_key)
                    chunk_uri = chunk.uri
                    self.debug('Downloading ' + chunk_uri)
                    if not (chunk_uri.startswith("https://") or chunk_uri.startswith("http://")):
                        chunk_uri = '/'.join(url_ref[0].split('.m3u8')[0].split('/')[:-1]) + '/' + chunk_uri
                    try:
                        m = session.get(chunk_uri, headers=self.headers, cookies=self.cookies, timeout=30)
                    except Exception as e:
                        if within_grace():
                            if chunk_key_was_new:
                                downloaded_set.discard(chunk_key)
                            if try_refresh_url():
                                break
                            self.logger.warning('HLS chunk fetch failed (transient, retrying): %s', e)
                            sleep(float(HLS_RETRY_SLEEP_SECONDS))
                            break
                        self.logger.warning('HLS chunk fetch failed (giving up): %s', e)
                        error = True
                        return
                    if m.status_code != 200:
                        if within_grace():
                            if chunk_key_was_new:
                                downloaded_set.discard(chunk_key)
                            if try_refresh_url():
                                break
                            self.logger.warning('HLS chunk status %s (transient, retrying)', m.status_code)
                            sleep(float(HLS_RETRY_SLEEP_SECONDS))
                            break
                        error = True
                        return
                    # Reject incomplete chunk (e.g. stream ended mid-transfer / private show) so we don't
                    # write a truncated fMP4 fragment and corrupt the file (ffmpeg "error reading header").
                    # In segment mode: close this segment immediately so we never write a "gap" in the middle
                    # (which would produce a large file where only the part before the gap is playable).
                    content_length = m.headers.get('Content-Length')
                    if content_length is not None:
                        try:
                            expected = int(content_length)
                            if len(m.content) != expected:
                                if chunk_key_was_new:
                                    downloaded_set.discard(chunk_key)
                                self.logger.warning(
                                    'HLS chunk incomplete (got %s bytes, expected %s); closing segment to keep file valid',
                                    len(m.content), expected
                                )
                                if segment_during_download:
                                    if current_file:
                                        _mark_problematic_file(
                                            current_file,
                                            f'incomplete chunk detected: got={len(m.content)} expected={expected}',
                                        )
                                    # Force rotation on next write and continue download.
                                    current_file_size = int(segment_size_bytes)
                                    break
                                if within_grace():
                                    sleep(float(HLS_RETRY_SLEEP_SECONDS))
                                break
                        except (ValueError, TypeError):
                            pass
                    # fMP4 init chunks:
                    # - keep only the latest init bytes (don't append forever)
                    # - write init only once per output file to avoid duplicate moov atoms
                    if segment_during_download and is_init_chunk:
                        if not _looks_like_mp4_fragment_bytes(m.content):
                            if chunk_key_was_new:
                                downloaded_set.discard(chunk_key)
                            self.logger.warning(
                                'Rejected suspicious HLS init chunk (uri=%s, bytes=%s)',
                                chunk_uri, len(m.content)
                            )
                            if current_file:
                                _mark_problematic_file(
                                    current_file,
                                    f'rejected suspicious init chunk: {chunk_uri}',
                                )
                            if within_grace():
                                if try_refresh_url():
                                    break
                                sleep(float(HLS_RETRY_SLEEP_SECONDS))
                                break
                            error = True
                            return
                        if playlist_init_parts is not None:
                            playlist_init_parts[i] = m.content
                            if all(part is not None for part in playlist_init_parts):
                                init_segment_bytes[0] = b''.join(playlist_init_parts)
                        if current_file_has_init[0]:
                            last_success = time.monotonic()
                            if self.stopDownloadFlag:
                                return
                            continue
                        file_handle = get_output_file()
                        file_handle.write(m.content)
                        last_success = time.monotonic()
                        current_file_size += len(m.content)
                        if (i + 1) >= n_init:
                            current_file_has_init[0] = True
                        if self.stopDownloadFlag:
                            return
                        continue
                    playlist_uses_fmp4 = n_init > 0
                    if playlist_uses_fmp4:
                        payload_ok = _looks_like_mp4_fragment_bytes(m.content)
                    else:
                        payload_ok = _looks_like_mpegts_bytes(m.content)
                    if not payload_ok:
                        if chunk_key_was_new:
                            downloaded_set.discard(chunk_key)
                        self.logger.warning(
                            'Rejected suspicious HLS media chunk (uri=%s, bytes=%s, mode=%s)',
                            chunk_uri, len(m.content), 'fmp4' if playlist_uses_fmp4 else 'mpegts'
                        )
                        if segment_during_download and current_file:
                            _mark_problematic_file(
                                current_file,
                                f'rejected suspicious media chunk: {chunk_uri}',
                            )
                            # Close this segment now so bad data cannot contaminate it,
                            # but keep download loop alive.
                            current_file_size = int(segment_size_bytes)
                            break
                        if within_grace():
                            if try_refresh_url():
                                break
                            sleep(float(HLS_RETRY_SLEEP_SECONDS))
                            break
                        error = True
                        return
                    file_handle = get_output_file()
                    file_handle.write(m.content)
                    last_success = time.monotonic()
                    if segment_during_download:
                        current_file_size += len(m.content)
                        current_file_has_media[0] = True
                    if self.stopDownloadFlag:
                        return
                if not did_download:
                    sleep(10)
        except Exception as e:
            error = True
            self.logger.error('Unexpected error in HLS download loop: %s', e, exc_info=True)
        finally:
            if outfile[0] and not outfile[0].closed:
                try:
                    outfile[0].flush()
                    try:
                        os.fsync(outfile[0].fileno())
                    except (OSError, AttributeError):
                        pass
                except (OSError, ValueError):
                    pass
                outfile[0].close()

    def terminate():
        self.stopDownloadFlag = True

    process = Thread(target=execute)
    process.start()
    self.stopDownload = terminate
    process.join()
    self.stopDownload = None

    # Post-processing for segment mode: always convert/clean last .ts (even on error) so we don't leave .ts files
    if segment_during_download:
        # Wait for all background segment conversions to finish before converting last segment,
        # so we avoid races (e.g. with convert_residual_ts_to_mp4) and return only when all conversions are done.
        for t in segment_convert_threads:
            t.join()
        # Wait a bit to ensure the last file is fully closed
        sleep(0.5)
        
        # Convert the last segment that might still be in .ts format (success or error path)
        if current_file and os.path.exists(current_file):
            last_sz = os.path.getsize(current_file)
            if last_sz > 0:
                final_filename = current_file.replace('.ts', '.' + CONTAINER)
                stderr_path = final_filename + '.postprocess_stderr.log'
                stderr_file = None
                input_tmp_path = None
                try:
                    if not current_file_has_media[0]:
                        _mark_problematic_file(
                            current_file,
                            'final segment has no media payload (init-only or invalid chunks)',
                        )
                        self.logger.warning('Final segment kept for inspection (no media payload): %s', current_file)
                        return successful_outputs[0] > 0
                    # Always write stderr log so it can be inspected when conversion fails (e.g. exit 254)
                    ffmpeg_input_path, input_tmp_path = _safe_make_input_with_init_if_needed(current_file)
                    has_streams = _probe_has_av_streams(ffmpeg_input_path)
                    if has_streams is False:
                        if input_tmp_path:
                            try:
                                os.remove(input_tmp_path)
                            except OSError:
                                pass
                        _mark_problematic_file(
                            current_file,
                            'ffprobe detected no audio/video streams for final segment conversion',
                        )
                        self.logger.warning('Final segment has no streams; kept for inspection: %s', current_file)
                    else:
                        stdout = open(final_filename + '.postprocess_stdout.log', 'w+') if DEBUG else subprocess.DEVNULL
                        stderr_file = open(stderr_path, 'w+')
                        output_str = '-c:a copy -c:v copy'
                        ff = FFmpeg(executable=FFMPEG_PATH, inputs={ffmpeg_input_path: None}, outputs={final_filename: output_str})
                        ff.run(stdout=stdout, stderr=stderr_file)
                        stderr_file.close()
                        stderr_file = None
                        try:
                            os.remove(stderr_path)
                        except OSError:
                            pass
                        if input_tmp_path:
                            try:
                                os.remove(input_tmp_path)
                            except OSError:
                                pass
                        os.remove(current_file)
                        _rename_mp4_by_title(final_filename, self.logger)
                        successful_outputs[0] += 1
                except FFRuntimeError as e:
                    if stderr_file:
                        try:
                            stderr_file.close()
                        except OSError:
                            pass
                    try:
                        if os.path.exists(final_filename):
                            os.remove(final_filename)
                    except OSError:
                        pass
                    ffmpeg_input_path, input_tmp_path = _safe_make_input_with_init_if_needed(current_file)
                    # Retry once with corruption-tolerant demux flags to salvage partial final segment.
                    recovered = False
                    try:
                        with open(stderr_path, 'a+') as fallback_stderr:
                            recovered = _run_recovery_remux(
                                ffmpeg_input_path,
                                final_filename,
                                fallback_stderr,
                            )
                        if not recovered:
                            raise RuntimeError("Recovery remux failed")
                        try:
                            os.remove(stderr_path)
                        except OSError:
                            pass
                        if input_tmp_path:
                            try:
                                os.remove(input_tmp_path)
                            except OSError:
                                pass
                        os.remove(current_file)
                        _rename_mp4_by_title(final_filename, self.logger)
                        successful_outputs[0] += 1
                        recovered = True
                    except Exception:
                        pass
                    if input_tmp_path:
                        try:
                            os.remove(input_tmp_path)
                        except OSError:
                            pass
                    if (not recovered) and e.exit_code:
                        self.logger.error(f'Error converting final segment (exit_code=%s): %s. Check {stderr_path!r} for ffmpeg stderr.', e.exit_code, e)
                    if not recovered:
                        _mark_problematic_file(
                            current_file,
                            f'ffmpeg final segment remux failed (exit_code={getattr(e, "exit_code", "unknown")})',
                        )
                except Exception as e:
                    if stderr_file:
                        try:
                            stderr_file.close()
                        except OSError:
                            pass
                    if input_tmp_path:
                        try:
                            os.remove(input_tmp_path)
                        except OSError:
                            pass
                    self.logger.error(f'Unexpected error converting final segment: {e}. Check {stderr_path!r} for ffmpeg stderr.')
                    try:
                        if os.path.exists(final_filename):
                            os.remove(final_filename)
                    except OSError:
                        pass
            else:
                try:
                    os.remove(current_file)
                except OSError:
                    pass
        
        if error:
            return False
        # Report success only if at least one final output file was created.
        return successful_outputs[0] > 0
    elif error:
        return False
    else:
        # Original behavior: single file post-processing
        if not os.path.exists(tmpfilename):
            return False

        if os.path.getsize(tmpfilename) == 0:
            os.remove(tmpfilename)
            return False

        try:
            has_streams = _probe_has_av_streams(tmpfilename)
            if has_streams is False:
                _mark_problematic_file(
                    tmpfilename,
                    'ffprobe detected no audio/video streams during final conversion',
                )
                self.logger.warning('Temp file has no streams; kept for inspection: %s', tmpfilename)
                return False
            stdout = open(filename + '.postprocess_stdout.log', 'w+') if DEBUG else subprocess.DEVNULL
            stderr = open(filename + '.postprocess_stderr.log', 'w+') if DEBUG else subprocess.DEVNULL
            output_str = '-c:a copy -c:v copy'
            ff = FFmpeg(executable=FFMPEG_PATH, inputs={tmpfilename: None}, outputs={filename: output_str})
            ff.run(stdout=stdout, stderr=stderr)
            os.remove(tmpfilename)
            _rename_mp4_by_title(filename, self.logger)
        except FFRuntimeError as e:
            try:
                if os.path.exists(filename):
                    os.remove(filename)
            except OSError:
                pass
            try:
                if not _run_recovery_remux(tmpfilename, filename, subprocess.DEVNULL):
                    raise RuntimeError("Recovery remux failed")
                os.remove(tmpfilename)
                _rename_mp4_by_title(filename, self.logger)
            except Exception:
                if e.exit_code:
                    self.logger.error('Final remux failed (exit_code=%s)', e.exit_code)
                _mark_problematic_file(
                    tmpfilename,
                    f'ffmpeg final remux failed (exit_code={getattr(e, "exit_code", "unknown")})',
                )
                return False

    return True
