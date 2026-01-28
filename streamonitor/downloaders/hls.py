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
    conversion_threads = []  # Track all conversion threads to wait for completion
    
    if segment_during_download:
        # Use the initial filename for first segment (but as .ts)
        current_file = filename[:-len('.' + CONTAINER)] + '.ts'
        current_file_size = 0

    def execute():
        nonlocal error, current_file, current_file_size, segment_files, conversion_threads
        downloaded_list = []
        outfile = [None]  # Use list to allow modification in nested function
        
        def convert_segment_to_mp4(ts_file_path, final_filename):
            """Convert a .ts segment file to final format (MP4) in background thread"""
            def convert():
                stderr_log_path = None
                try:
                    # Wait a bit to ensure file is fully written and closed
                    sleep(0.5)
                    
                    if not os.path.exists(ts_file_path):
                        return
                    if os.path.getsize(ts_file_path) == 0:
                        os.remove(ts_file_path)
                        return
                    
                    stderr_log_path = final_filename + '.postprocess_stderr.log'
                    stdout = open(final_filename + '.postprocess_stdout.log', 'w+') if DEBUG else subprocess.DEVNULL
                    stderr = open(stderr_log_path, 'w+') if DEBUG else subprocess.DEVNULL
                    output_str = '-c:a copy -c:v copy'
                    if CONTAINER == 'mp4':
                        output_str += ' -movflags +faststart'
                    
                    # 使用错误恢复选项来处理可能损坏的文件
                    input_options = '-err_detect ignore_err'
                    ff = FFmpeg(executable=FFMPEG_PATH, inputs={ts_file_path: input_options}, outputs={final_filename: output_str})
                    ff.run(stdout=stdout, stderr=stderr)
                    
                    # 检查输出文件是否成功创建
                    if not os.path.exists(final_filename) or os.path.getsize(final_filename) == 0:
                        self.logger.warning(f'Conversion produced empty or missing file: {final_filename}')
                        return
                    
                    os.remove(ts_file_path)
                except FFRuntimeError as e:
                    # 读取 stderr 日志以获取详细错误信息
                    error_details = ""
                    if stderr_log_path and os.path.exists(stderr_log_path):
                        try:
                            with open(stderr_log_path, 'r', encoding='utf-8', errors='ignore') as f:
                                stderr_content = f.read()
                                if stderr_content:
                                    lines = stderr_content.strip().split('\n')
                                    error_details = '\n'.join(lines[-5:]) if len(lines) > 5 else stderr_content
                        except Exception:
                            pass
                    
                    if e.exit_code and e.exit_code != 255:
                        error_msg = f'Error converting segment {os.path.basename(ts_file_path)}'
                        if e.exit_code:
                            error_msg += f' (exit code: {e.exit_code})'
                        if error_details:
                            error_msg += f'\nFFmpeg error: {error_details}'
                        else:
                            error_msg += f': {e}'
                        self.logger.error(error_msg)
                        # Keep the .ts file if conversion fails so it can be retried later
                except Exception as e:
                    self.logger.error(f'Unexpected error converting segment {os.path.basename(ts_file_path)}: {e}')
                    # Keep the .ts file if conversion fails so it can be retried later
            
            # Run conversion in background thread (like pause/resume does)
            convert_thread = Thread(target=convert, daemon=False)  # Non-daemon so it completes even if main thread exits
            convert_thread.start()
            conversion_threads.append(convert_thread)  # Track the thread
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
                    if os.path.exists(prev_ts_file) and os.path.getsize(prev_ts_file) > 0:
                        segment_files.append((prev_ts_file, prev_final_filename))
                        convert_segment_to_mp4(prev_ts_file, prev_final_filename)
                    
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
                r = session.get(url, headers=self.headers, cookies=self.cookies, timeout=30)
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
                    m = session.get(chunk_uri, headers=self.headers, cookies=self.cookies, timeout=30)
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
                outfile[0].close()

    def terminate():
        self.stopDownloadFlag = True

    process = Thread(target=execute)
    process.start()
    self.stopDownload = terminate
    process.join()
    self.stopDownload = None

    if error:
        return False

    # Post-processing: convert the last segment file to final format (if still exists)
    if segment_during_download:
        # Wait a bit to ensure the last file is fully closed
        sleep(0.5)
        
        # Convert the last segment that might still be in .ts format
        if current_file and os.path.exists(current_file) and os.path.getsize(current_file) > 0:
            final_filename = current_file.replace('.ts', '.' + CONTAINER)
            stderr_log_path = final_filename + '.postprocess_stderr.log'
            try:
                stdout = open(final_filename + '.postprocess_stdout.log', 'w+') if DEBUG else subprocess.DEVNULL
                stderr = open(stderr_log_path, 'w+') if DEBUG else subprocess.DEVNULL
                output_str = '-c:a copy -c:v copy'
                if CONTAINER == 'mp4':
                    output_str += ' -movflags +faststart'
                
                # 使用错误恢复选项来处理可能损坏的文件
                input_options = '-err_detect ignore_err'
                ff = FFmpeg(executable=FFMPEG_PATH, inputs={current_file: input_options}, outputs={final_filename: output_str})
                ff.run(stdout=stdout, stderr=stderr)
                
                # 检查输出文件是否成功创建
                if not os.path.exists(final_filename) or os.path.getsize(final_filename) == 0:
                    self.logger.warning(f'Conversion produced empty or missing file: {final_filename}')
                else:
                    os.remove(current_file)
            except FFRuntimeError as e:
                # 读取 stderr 日志以获取详细错误信息
                error_details = ""
                if stderr_log_path and os.path.exists(stderr_log_path):
                    try:
                        with open(stderr_log_path, 'r', encoding='utf-8', errors='ignore') as f:
                            stderr_content = f.read()
                            if stderr_content:
                                lines = stderr_content.strip().split('\n')
                                error_details = '\n'.join(lines[-5:]) if len(lines) > 5 else stderr_content
                    except Exception:
                        pass
                
                if e.exit_code and e.exit_code != 255:
                    error_msg = f'Error converting final segment {os.path.basename(current_file)}'
                    if e.exit_code:
                        error_msg += f' (exit code: {e.exit_code})'
                    if error_details:
                        error_msg += f'\nFFmpeg error: {error_details}'
                    else:
                        error_msg += f': {e}'
                    self.logger.error(error_msg)
                    # Keep the .ts file if conversion fails so it can be retried later
            except Exception as e:
                self.logger.error(f'Unexpected error converting final segment {os.path.basename(current_file)}: {e}')
                # Keep the .ts file if conversion fails so it can be retried later
        
        # Wait for all conversion threads to complete (with timeout)
        # This ensures .ts files are converted before the function returns
        import time
        max_wait_time = 300  # Maximum 5 minutes to wait for conversions
        start_time = time.time()
        remaining_threads = [t for t in conversion_threads if t.is_alive()]
        
        while remaining_threads and (time.time() - start_time) < max_wait_time:
            sleep(1)
            remaining_threads = [t for t in conversion_threads if t.is_alive()]
            if remaining_threads:
                self.logger.debug(f'Waiting for {len(remaining_threads)} conversion thread(s) to complete...')
        
        if remaining_threads:
            self.logger.warning(f'{len(remaining_threads)} conversion thread(s) did not complete in time. '
                              f'Some .ts files may remain and can be converted later using convert_ts_files.py')
        
        # Check if at least one segment was created
        return current_file is not None
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
