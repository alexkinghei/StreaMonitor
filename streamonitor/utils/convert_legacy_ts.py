"""
工具模块：在后台转换遗留的 .ts 文件
"""
import os
import subprocess
import time
from pathlib import Path
from threading import Thread
from ffmpy import FFmpeg, FFRuntimeError
from parameters import DOWNLOADS_DIR, CONTAINER, FFMPEG_PATH, DEBUG
import streamonitor.log as log


def convert_ts_file(ts_file_path, final_filename, logger=None):
    """转换单个 .ts 文件到最终格式"""
    stderr_log_path = None
    try:
        if not os.path.exists(ts_file_path):
            return False
        
        # 检查文件是否正在被写入（正在录制）
        # 如果文件在最近30秒内被修改，认为它可能正在被录制，跳过转换
        try:
            file_mtime = os.path.getmtime(ts_file_path)
            time_since_modification = time.time() - file_mtime
            if time_since_modification < 30:  # 30秒内被修改，可能正在录制
                if logger:
                    logger.debug(f'Skipping {os.path.basename(ts_file_path)}: file was modified {time_since_modification:.1f}s ago (may be recording)')
                return False
        except Exception:
            pass  # 如果无法获取修改时间，继续处理
        
        # 尝试以只读模式打开文件，检查是否被锁定（正在被写入）
        try:
            with open(ts_file_path, 'rb') as test_file:
                test_file.read(1)  # 尝试读取一个字节
        except (IOError, OSError, PermissionError):
            # 文件被锁定或无法读取，可能正在被写入，跳过
            if logger:
                logger.debug(f'Skipping {os.path.basename(ts_file_path)}: file is locked (may be recording)')
            return False
        
        file_size = os.path.getsize(ts_file_path)
        if file_size == 0:
            os.remove(ts_file_path)
            return False
        
        # 如果目标文件已存在，删除 .ts 文件
        if os.path.exists(final_filename):
            os.remove(ts_file_path)
            return True
        
        # 创建日志文件路径
        # 即使 DEBUG=False，也创建 stderr 日志文件用于错误诊断
        stderr_log_path = final_filename + '.postprocess_stderr.log'
        stdout_log_path = final_filename + '.postprocess_stdout.log'
        
        stdout = open(stdout_log_path, 'w+') if DEBUG else subprocess.DEVNULL
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
            
            ff = FFmpeg(
                executable=FFMPEG_PATH,
                inputs={ts_file_path: input_options},
                outputs={final_filename: output_str}
            )
            ff.run(stdout=stdout, stderr=stderr_file)
            
            # 关闭 stderr 文件以便后续读取
            stderr_file.close()
            stderr_file = None
            
            # 检查输出文件是否成功创建
            if not os.path.exists(final_filename) or os.path.getsize(final_filename) == 0:
                if logger:
                    logger.warning(f'Conversion produced empty or missing file: {final_filename}')
                return False
            
            os.remove(ts_file_path)
            if logger:
                logger.debug(f'Converted legacy .ts file: {os.path.basename(ts_file_path)} -> {os.path.basename(final_filename)}')
            return True
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
                        # 提取最后几行错误信息
                        lines = stderr_content.strip().split('\n')
                        error_details = '\n'.join(lines[-5:]) if len(lines) > 5 else stderr_content
            except Exception:
                pass
        
        # 如果文件没有有效流，删除损坏的文件
        if has_no_streams:
            try:
                if os.path.exists(ts_file_path):
                    os.remove(ts_file_path)
                    if logger:
                        logger.warning(f'Removed invalid .ts file (no streams): {os.path.basename(ts_file_path)}')
            except Exception:
                pass
            return False
        
        if logger:
            error_msg = f'Failed to convert {os.path.basename(ts_file_path)}'
            if e.exit_code:
                error_msg += f' (exit code: {e.exit_code})'
            if error_details:
                error_msg += f'\nFFmpeg error: {error_details}'
            else:
                error_msg += f': {e}'
            logger.warning(error_msg)
        return False
    except Exception as e:
        if logger:
            logger.warning(f'Unexpected error converting {os.path.basename(ts_file_path)}: {e}')
        return False


def convert_legacy_ts_files_background():
    """在后台线程中转换所有遗留的 .ts 文件"""
    def convert_all():
        logger = log.Logger("ts_converter")
        downloads_path = Path(DOWNLOADS_DIR)
        
        if not downloads_path.exists():
            return
        
        # 查找所有 .ts 文件（排除临时文件）
        ts_files = [f for f in downloads_path.rglob("*.ts") if not f.name.endswith('.tmp.ts')]
        
        if not ts_files:
            return
        
        logger.info(f'Found {len(ts_files)} legacy .ts file(s), converting in background...')
        
        success_count = 0
        skipped_count = 0
        for ts_file in ts_files:
            final_filename = str(ts_file).replace('.ts', '.' + CONTAINER)
            result = convert_ts_file(str(ts_file), final_filename, logger)
            if result:
                success_count += 1
            elif result is False and os.path.exists(str(ts_file)):
                # 如果转换失败但文件仍然存在，可能是被跳过了（正在录制）
                # 检查文件是否在最近被修改
                try:
                    file_mtime = os.path.getmtime(str(ts_file))
                    time_since_modification = time.time() - file_mtime
                    if time_since_modification < 30:
                        skipped_count += 1
                except Exception:
                    pass
        
        if success_count > 0:
            logger.info(f'Converted {success_count} legacy .ts file(s) to {CONTAINER} format')
        if skipped_count > 0:
            logger.debug(f'Skipped {skipped_count} .ts file(s) that may be currently recording')
    
    # 在后台线程中运行，不阻塞主程序
    thread = Thread(target=convert_all, daemon=True)
    thread.start()
    return thread
