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
        from pycurl_requests.exceptions import ConnectionError, Timeout, RequestException
        _http_lib = 'pycurl'
    except ImportError:
        pass
if not _http_lib:
    try:
        import requests
        from requests.exceptions import ConnectionError, Timeout, RequestException
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
        
        def convert_segment_to_mp4(ts_file_path, final_filename, sync=False):
            """Convert a .ts segment file to final format (MP4)
            
            Args:
                ts_file_path: Path to the .ts file
                final_filename: Target output filename
                sync: If True, convert synchronously (block until done). If False, convert in background thread.
            """
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
                    # 总是捕获 stderr 以便诊断错误
                    stderr_file = open(stderr_log_path, 'w+')
                    try:
                        output_str = '-c:a copy -c:v copy'
                        if CONTAINER == 'mp4':
                            output_str += ' -movflags +faststart'
                        
                        # 明确指定输入格式为 mpegts（TS 格式），避免 FFmpeg 误识别
                        # 对于可能损坏的文件，添加错误恢复选项
                        # 增加 analyzeduration 和 probesize 以处理损坏的文件头部
                        input_options = '-f mpegts -err_detect ignore_err -analyzeduration 20000000 -probesize 50000000'
                        ff = FFmpeg(executable=FFMPEG_PATH, inputs={ts_file_path: input_options}, outputs={final_filename: output_str})
                        ff.run(stdout=stdout, stderr=stderr_file)
                        
                        # 关闭 stderr 文件以便后续读取
                        stderr_file.close()
                        stderr_file = None
                        
                        # 检查输出文件是否成功创建
                        if not os.path.exists(final_filename) or os.path.getsize(final_filename) == 0:
                            self.logger.warning(f'Conversion produced empty or missing file: {final_filename}')
                            return
                        
                        os.remove(ts_file_path)
                    finally:
                        if stderr_file:
                            stderr_file.close()
                        if stdout != subprocess.DEVNULL:
                            stdout.close()
                except FFRuntimeError as e:
                    # 读取 stderr 日志以获取详细错误信息
                    error_details = ""
                    has_no_streams = False
                    if stderr_log_path and os.path.exists(stderr_log_path):
                        try:
                            with open(stderr_log_path, 'r', encoding='utf-8', errors='ignore') as f:
                                stderr_content = f.read()
                                if stderr_content:
                                    # 检查是否是"没有流"的错误
                                    if 'does not contain any stream' in stderr_content or 'no stream' in stderr_content.lower():
                                        has_no_streams = True
                                    lines = stderr_content.strip().split('\n')
                                    error_details = '\n'.join(lines[-5:]) if len(lines) > 5 else stderr_content
                        except Exception:
                            pass
                    
                    # 如果文件没有有效流，删除损坏的文件
                    if has_no_streams:
                        try:
                            if os.path.exists(ts_file_path):
                                os.remove(ts_file_path)
                                self.logger.warning(f'Removed invalid .ts segment (no streams): {os.path.basename(ts_file_path)}')
                        except Exception:
                            pass
                        return
                    
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
            
            if sync:
                # 同步转换：直接执行，用于断流时立即封装
                convert()
            else:
                # 异步转换：在后台线程中执行
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
                    # 检查文件大小，太小的文件可能不包含有效流（至少需要几KB）
                    if os.path.exists(prev_ts_file) and os.path.getsize(prev_ts_file) > 1024:  # 至少 1KB
                        segment_files.append((prev_ts_file, prev_final_filename))
                        convert_segment_to_mp4(prev_ts_file, prev_final_filename)
                    elif os.path.exists(prev_ts_file):
                        # 文件太小，可能是无效的，删除它
                        try:
                            os.remove(prev_ts_file)
                            self.logger.warning(f'Removed too small segment (likely invalid): {os.path.basename(prev_ts_file)}')
                        except Exception:
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
            consecutive_failures = 0  # 连续失败计数器
            max_consecutive_failures = 5  # 连续失败阈值，达到此值认为断流
            
            while not self.stopDownloadFlag:
                try:
                    r = session.get(url, headers=self.headers, cookies=self.cookies, timeout=30)
                    consecutive_failures = 0  # 成功获取播放列表，重置失败计数
                except (ConnectionError, Timeout, RequestException) as e:
                    consecutive_failures += 1
                    error_msg = str(e)
                    if len(error_msg) > 200:
                        error_msg = error_msg[:200] + '...'
                    self.logger.warning(f'Network error fetching playlist ({type(e).__name__}): {error_msg} (连续失败: {consecutive_failures}/{max_consecutive_failures})')
                    
                    # 检测到断流：立即封装并转换当前文件
                    if consecutive_failures >= max_consecutive_failures:
                        self.logger.warning(f'检测到断流（连续失败 {consecutive_failures} 次），立即封装已录制内容...')
                        # 关闭当前文件
                        if outfile[0] and not outfile[0].closed:
                            outfile[0].flush()
                            outfile[0].close()
                        
                        # 如果当前文件有足够内容，立即同步转换
                        if segment_during_download and current_file and os.path.exists(current_file):
                            file_size = os.path.getsize(current_file)
                            if file_size > 1024:  # 至少 1KB
                                final_filename = current_file.replace('.ts', '.' + CONTAINER)
                                self.logger.info(f'正在转换当前段（{file_size} 字节）: {os.path.basename(current_file)} -> {os.path.basename(final_filename)}')
                                convert_segment_to_mp4(current_file, final_filename, sync=True)
                                self.logger.info(f'断流时已成功封装: {os.path.basename(final_filename)}')
                            else:
                                # 文件太小，删除
                                try:
                                    os.remove(current_file)
                                    self.logger.warning(f'删除过小的段文件: {os.path.basename(current_file)}')
                                except Exception:
                                    pass
                        
                        # 重置文件状态，准备重连后创建新文件
                        if segment_during_download:
                            new_filename = self.genOutFilename(create_dir=True)
                            current_file = new_filename[:-len('.' + CONTAINER)] + '.ts'
                            current_file_size = 0
                        
                        # 等待一段时间后尝试重连
                        self.logger.info(f'等待 10 秒后尝试重新连接...')
                        sleep(10)
                        consecutive_failures = 0  # 重置计数器，准备重连
                        continue
                    
                    sleep(5)  # Wait before retrying
                    continue
                
                content = r.content.decode("utf-8")
                if m3u_processor:
                    content = m3u_processor(content)
                chunklist = m3u8.loads(content)
                if len(chunklist.segments) == 0:
                    return
                
                chunk_failures = 0  # 本次循环中的chunk下载失败计数
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
                        chunk_failures = 0  # 成功下载，重置chunk失败计数
                    except (ConnectionError, Timeout, RequestException) as e:
                        chunk_failures += 1
                        consecutive_failures += 1
                        error_msg = str(e)
                        if len(error_msg) > 200:
                            error_msg = error_msg[:200] + '...'
                        self.logger.warning(f'Network error downloading chunk ({type(e).__name__}): {error_msg} (连续失败: {consecutive_failures}/{max_consecutive_failures})')
                        
                        # 检测到断流：立即封装并转换当前文件
                        if consecutive_failures >= max_consecutive_failures:
                            self.logger.warning(f'检测到断流（连续失败 {consecutive_failures} 次），立即封装已录制内容...')
                            # 关闭当前文件
                            if outfile[0] and not outfile[0].closed:
                                outfile[0].flush()
                                outfile[0].close()
                            
                            # 如果当前文件有足够内容，立即同步转换
                            if segment_during_download and current_file and os.path.exists(current_file):
                                file_size = os.path.getsize(current_file)
                                if file_size > 1024:  # 至少 1KB
                                    final_filename = current_file.replace('.ts', '.' + CONTAINER)
                                    self.logger.info(f'正在转换当前段（{file_size} 字节）: {os.path.basename(current_file)} -> {os.path.basename(final_filename)}')
                                    convert_segment_to_mp4(current_file, final_filename, sync=True)
                                    self.logger.info(f'断流时已成功封装: {os.path.basename(final_filename)}')
                                else:
                                    # 文件太小，删除
                                    try:
                                        os.remove(current_file)
                                        self.logger.warning(f'删除过小的段文件: {os.path.basename(current_file)}')
                                    except Exception:
                                        pass
                            
                            # 重置文件状态，准备重连后创建新文件
                            if segment_during_download:
                                new_filename = self.genOutFilename(create_dir=True)
                                current_file = new_filename[:-len('.' + CONTAINER)] + '.ts'
                                current_file_size = 0
                            
                            # 等待一段时间后尝试重连
                            self.logger.info(f'等待 10 秒后尝试重新连接...')
                            sleep(10)
                            consecutive_failures = 0  # 重置计数器，准备重连
                            break  # 跳出chunk循环，重新获取播放列表
                        
                        # 如果当前文件很小（可能不完整），删除它
                        if segment_during_download and current_file and os.path.exists(current_file):
                            try:
                                if os.path.getsize(current_file) < 1024:  # 小于 1KB 可能是无效文件
                                    os.remove(current_file)
                                    self.logger.warning(f'Removed incomplete segment due to network error: {os.path.basename(current_file)}')
                            except Exception:
                                pass
                        continue  # Skip this chunk and try next one
                    
                    if m.status_code != 200:
                        chunk_failures += 1
                        consecutive_failures += 1
                        # HTTP 错误时，如果当前文件很小（可能不完整），删除它
                        if segment_during_download and current_file and os.path.exists(current_file):
                            try:
                                if os.path.getsize(current_file) < 1024:  # 小于 1KB 可能是无效文件
                                    os.remove(current_file)
                                    self.logger.warning(f'Removed incomplete segment due to HTTP error: {os.path.basename(current_file)}')
                            except Exception:
                                pass
                        
                        # 检测到断流
                        if consecutive_failures >= max_consecutive_failures:
                            self.logger.warning(f'检测到断流（连续失败 {consecutive_failures} 次），立即封装已录制内容...')
                            # 关闭当前文件
                            if outfile[0] and not outfile[0].closed:
                                outfile[0].flush()
                                outfile[0].close()
                            
                            # 如果当前文件有足够内容，立即同步转换
                            if segment_during_download and current_file and os.path.exists(current_file):
                                file_size = os.path.getsize(current_file)
                                if file_size > 1024:  # 至少 1KB
                                    final_filename = current_file.replace('.ts', '.' + CONTAINER)
                                    self.logger.info(f'正在转换当前段（{file_size} 字节）: {os.path.basename(current_file)} -> {os.path.basename(final_filename)}')
                                    convert_segment_to_mp4(current_file, final_filename, sync=True)
                                    self.logger.info(f'断流时已成功封装: {os.path.basename(final_filename)}')
                                else:
                                    # 文件太小，删除
                                    try:
                                        os.remove(current_file)
                                        self.logger.warning(f'删除过小的段文件: {os.path.basename(current_file)}')
                                    except Exception:
                                        pass
                            
                            # 重置文件状态，准备重连后创建新文件
                            if segment_during_download:
                                new_filename = self.genOutFilename(create_dir=True)
                                current_file = new_filename[:-len('.' + CONTAINER)] + '.ts'
                                current_file_size = 0
                            
                            # 等待一段时间后尝试重连
                            self.logger.info(f'等待 10 秒后尝试重新连接...')
                            sleep(10)
                            consecutive_failures = 0  # 重置计数器，准备重连
                            break  # 跳出chunk循环，重新获取播放列表
                        
                        continue  # Skip this chunk and try next one
                    
                    # 成功下载chunk，重置连续失败计数
                    consecutive_failures = 0
                    file_handle = get_output_file()
                    file_handle.write(m.content)
                    file_handle.flush()  # 立即刷新缓冲区，确保数据写入磁盘
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
        # 检查文件大小，太小的文件可能不包含有效流
        if current_file and os.path.exists(current_file) and os.path.getsize(current_file) > 1024:  # 至少 1KB
            final_filename = current_file.replace('.ts', '.' + CONTAINER)
            stderr_log_path = final_filename + '.postprocess_stderr.log'
            try:
                stdout = open(final_filename + '.postprocess_stdout.log', 'w+') if DEBUG else subprocess.DEVNULL
                # 总是捕获 stderr 以便诊断错误
                stderr_file = open(stderr_log_path, 'w+')
                try:
                    output_str = '-c:a copy -c:v copy'
                    if CONTAINER == 'mp4':
                        output_str += ' -movflags +faststart'
                    
                    # 明确指定输入格式为 mpegts（TS 格式），避免 FFmpeg 误识别
                    # 对于可能损坏的文件，添加错误恢复选项
                    # 增加 analyzeduration 和 probesize 以处理损坏的文件头部
                    input_options = '-f mpegts -err_detect ignore_err -analyzeduration 20000000 -probesize 50000000'
                    ff = FFmpeg(executable=FFMPEG_PATH, inputs={current_file: input_options}, outputs={final_filename: output_str})
                    ff.run(stdout=stdout, stderr=stderr_file)
                    
                    # 关闭 stderr 文件以便后续读取
                    stderr_file.close()
                    stderr_file = None
                    
                    # 检查输出文件是否成功创建
                    if not os.path.exists(final_filename) or os.path.getsize(final_filename) == 0:
                        self.logger.warning(f'Conversion produced empty or missing file: {final_filename}')
                    else:
                        os.remove(current_file)
                finally:
                    if stderr_file:
                        stderr_file.close()
                    if stdout != subprocess.DEVNULL:
                        stdout.close()
            except FFRuntimeError as e:
                # 读取 stderr 日志以获取详细错误信息
                error_details = ""
                has_no_streams = False
                if stderr_log_path and os.path.exists(stderr_log_path):
                    try:
                        with open(stderr_log_path, 'r', encoding='utf-8', errors='ignore') as f:
                            stderr_content = f.read()
                            if stderr_content:
                                # 检查是否是"没有流"的错误
                                if 'does not contain any stream' in stderr_content or 'no stream' in stderr_content.lower():
                                    has_no_streams = True
                                lines = stderr_content.strip().split('\n')
                                error_details = '\n'.join(lines[-5:]) if len(lines) > 5 else stderr_content
                    except Exception:
                        pass
                
                # 如果文件没有有效流，删除损坏的文件
                if has_no_streams:
                    try:
                        if current_file and os.path.exists(current_file):
                            os.remove(current_file)
                            self.logger.warning(f'Removed invalid final .ts segment (no streams): {os.path.basename(current_file)}')
                    except Exception:
                        pass
                    return
                
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
            # 明确指定输入格式为 mpegts（TS 格式），避免 FFmpeg 误识别
            # 对于可能损坏的文件，添加错误恢复选项
            # 增加 analyzeduration 和 probesize 以处理损坏的文件头部
            input_options = '-f mpegts -err_detect ignore_err -analyzeduration 20000000 -probesize 50000000'
            ff = FFmpeg(executable=FFMPEG_PATH, inputs={tmpfilename: input_options}, outputs={filename: output_str})
            ff.run(stdout=stdout, stderr=stderr)
            os.remove(tmpfilename)
        except FFRuntimeError as e:
            if e.exit_code and e.exit_code != 255:
                return False

    return True
