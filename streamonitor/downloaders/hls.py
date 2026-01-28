import m3u8
import os
import subprocess
from threading import Thread
from ffmpy import FFmpeg, FFRuntimeError
from time import sleep
from parameters import DEBUG, CONTAINER, SEGMENT_SIZE, parse_segment_size, FFMPEG_PATH

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

# Segments smaller than this are discarded (no real content); avoids ~262B empty/minimal MP4s
MIN_SEGMENT_SIZE = 64 * 1024  # 64 KiB


def getVideoNativeHLS(self, url, filename, m3u_processor=None):
    self.stopDownloadFlag = False
    error = False
    tmpfilename = filename[:-len('.' + CONTAINER)] + '.tmp.ts'
    session = requests.Session()
    
    # Check if we should segment during download
    segment_size_bytes = parse_segment_size(SEGMENT_SIZE)
    segment_during_download = segment_size_bytes is not None
    current_file = None
    current_file_size = 0
    segment_files = []  # Track all segment files for final conversion
    
    if segment_during_download:
        # Use the initial filename for first segment (but as .ts)
        current_file = filename[:-len('.' + CONTAINER)] + '.ts'
        current_file_size = 0

    def execute():
        nonlocal error, current_file, current_file_size, segment_files
        downloaded_list = []
        outfile = [None]  # Use list to allow modification in nested function
        
        def convert_segment_to_mp4(ts_file_path, final_filename):
            """Convert a .ts segment file to final format (MP4) in background thread"""
            def convert():
                try:
                    # Wait a bit to ensure file is fully written and closed
                    sleep(0.5)
                    
                    if not os.path.exists(ts_file_path):
                        return
                    sz = os.path.getsize(ts_file_path)
                    if sz == 0 or sz < MIN_SEGMENT_SIZE:
                        try:
                            os.remove(ts_file_path)
                        except OSError:
                            pass
                        return
                    
                    stdout = open(final_filename + '.postprocess_stdout.log', 'w+') if DEBUG else subprocess.DEVNULL
                    stderr = open(final_filename + '.postprocess_stderr.log', 'w+') if DEBUG else subprocess.DEVNULL
                    output_str = '-c:a copy -c:v copy'
                    if CONTAINER == 'mp4':
                        output_str += ' -movflags +faststart'
                    ff = FFmpeg(executable=FFMPEG_PATH, inputs={ts_file_path: None}, outputs={final_filename: output_str})
                    ff.run(stdout=stdout, stderr=stderr)
                    os.remove(ts_file_path)
                except FFRuntimeError as e:
                    if e.exit_code and e.exit_code != 255:
                        self.logger.error(f'Error converting segment: {e}')
                except Exception as e:
                    self.logger.error(f'Unexpected error converting segment: {e}')
            
            # Run conversion in background thread (like pause/resume does)
            convert_thread = Thread(target=convert)
            convert_thread.start()
            return convert_thread
        
        def get_output_file():
            nonlocal current_file, current_file_size, segment_files
            if segment_during_download:
                # Check if we need to start a new segment
                if current_file_size >= int(segment_size_bytes):
                    if outfile[0] and not outfile[0].closed:
                        outfile[0].flush()
                        outfile[0].close()
                    
                    # Convert the completed segment to MP4 (like pause/resume logic)
                    prev_ts_file = current_file
                    # Generate final filename by replacing .ts with .mp4
                    prev_final_filename = prev_ts_file.replace('.ts', '.' + CONTAINER)
                    if os.path.exists(prev_ts_file):
                        prev_sz = os.path.getsize(prev_ts_file)
                        if prev_sz >= MIN_SEGMENT_SIZE:
                            segment_files.append((prev_ts_file, prev_final_filename))
                            convert_segment_to_mp4(prev_ts_file, prev_final_filename)
                        elif prev_sz > 0:
                            try:
                                os.remove(prev_ts_file)
                            except OSError:
                                pass
                    
                    # Generate new filename with timestamp (like pause/resume)
                    new_filename = self.genOutFilename(create_dir=True)
                    current_file = new_filename[:-len('.' + CONTAINER)] + '.ts'
                    current_file_size = 0
                if outfile[0] is None or outfile[0].closed:
                    outfile[0] = open(current_file, 'ab')
                return outfile[0]
            else:
                if outfile[0] is None:
                    outfile[0] = open(tmpfilename, 'wb')
                return outfile[0]
        
        try:
            did_download = False
            while not self.stopDownloadFlag:
                try:
                    r = session.get(url, headers=self.headers, cookies=self.cookies, timeout=30)
                except Exception as e:
                    self.logger.warning('HLS playlist fetch failed (connection/timeout): %s', e)
                    error = True
                    return
                content = r.content.decode("utf-8")
                if m3u_processor:
                    content = m3u_processor(content)
                chunklist = m3u8.loads(content)
                if len(chunklist.segments) == 0:
                    return
                for chunk in chunklist.segment_map + chunklist.segments:
                    if chunk.uri in downloaded_list:
                        continue
                    did_download = True
                    downloaded_list.append(chunk.uri)
                    chunk_uri = chunk.uri
                    self.debug('Downloading ' + chunk_uri)
                    if not chunk_uri.startswith("https://"):
                        chunk_uri = '/'.join(url.split('.m3u8')[0].split('/')[:-1]) + '/' + chunk_uri
                    try:
                        m = session.get(chunk_uri, headers=self.headers, cookies=self.cookies, timeout=30)
                    except Exception as e:
                        self.logger.warning('HLS chunk fetch failed (connection/timeout): %s', e)
                        error = True
                        return
                    if m.status_code != 200:
                        return
                    file_handle = get_output_file()
                    file_handle.write(m.content)
                    if segment_during_download:
                        current_file_size += len(m.content)
                    if self.stopDownloadFlag:
                        return
                if not did_download:
                    sleep(10)
        finally:
            if outfile[0] and not outfile[0].closed:
                try:
                    outfile[0].flush()
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
        # Wait a bit to ensure the last file is fully closed
        sleep(0.5)
        
        # Convert the last segment that might still be in .ts format (success or error path)
        if current_file and os.path.exists(current_file):
            last_sz = os.path.getsize(current_file)
            if last_sz >= MIN_SEGMENT_SIZE:
                final_filename = current_file.replace('.ts', '.' + CONTAINER)
                try:
                    stdout = open(final_filename + '.postprocess_stdout.log', 'w+') if DEBUG else subprocess.DEVNULL
                    stderr = open(final_filename + '.postprocess_stderr.log', 'w+') if DEBUG else subprocess.DEVNULL
                    output_str = '-c:a copy -c:v copy'
                    if CONTAINER == 'mp4':
                        output_str += ' -movflags +faststart'
                    ff = FFmpeg(executable=FFMPEG_PATH, inputs={current_file: None}, outputs={final_filename: output_str})
                    ff.run(stdout=stdout, stderr=stderr)
                    os.remove(current_file)
                except FFRuntimeError as e:
                    if e.exit_code and e.exit_code != 255:
                        self.logger.error(f'Error converting final segment: {e}')
                except Exception as e:
                    self.logger.error(f'Unexpected error converting final segment: {e}')
            else:
                try:
                    os.remove(current_file)
                except OSError:
                    pass
        
        if error:
            return False
        # Check if at least one segment was created
        return current_file is not None
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
            stdout = open(filename + '.postprocess_stdout.log', 'w+') if DEBUG else subprocess.DEVNULL
            stderr = open(filename + '.postprocess_stderr.log', 'w+') if DEBUG else subprocess.DEVNULL
            output_str = '-c:a copy -c:v copy'
            ff = FFmpeg(executable=FFMPEG_PATH, inputs={tmpfilename: None}, outputs={filename: output_str})
            ff.run(stdout=stdout, stderr=stderr)
            os.remove(tmpfilename)
        except FFRuntimeError as e:
            if e.exit_code and e.exit_code != 255:
                return False

    return True
