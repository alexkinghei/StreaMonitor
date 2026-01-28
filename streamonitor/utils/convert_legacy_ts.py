"""
工具模块：在后台转换遗留的 .ts 文件
"""
import os
import subprocess
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
        
        file_size = os.path.getsize(ts_file_path)
        if file_size == 0:
            os.remove(ts_file_path)
            return False
        
        # 如果目标文件已存在，删除 .ts 文件
        if os.path.exists(final_filename):
            os.remove(ts_file_path)
            return True
        
        # 创建日志文件路径
        stderr_log_path = final_filename + '.postprocess_stderr.log'
        stdout_log_path = final_filename + '.postprocess_stdout.log'
        
        stdout = open(stdout_log_path, 'w+') if DEBUG else subprocess.DEVNULL
        stderr = open(stderr_log_path, 'w+') if DEBUG else subprocess.DEVNULL
        
        output_str = '-c:a copy -c:v copy'
        if CONTAINER == 'mp4':
            output_str += ' -movflags +faststart'
        
        # 对于可能损坏的文件，添加错误恢复选项
        # 使用 -err_detect ignore_err 来忽略一些错误并继续处理
        input_options = '-err_detect ignore_err'
        
        ff = FFmpeg(
            executable=FFMPEG_PATH,
            inputs={ts_file_path: input_options},
            outputs={final_filename: output_str}
        )
        ff.run(stdout=stdout, stderr=stderr)
        
        # 检查输出文件是否成功创建
        if not os.path.exists(final_filename) or os.path.getsize(final_filename) == 0:
            if logger:
                logger.warning(f'Conversion produced empty or missing file: {final_filename}')
            return False
        
        os.remove(ts_file_path)
        if logger:
            logger.debug(f'Converted legacy .ts file: {os.path.basename(ts_file_path)} -> {os.path.basename(final_filename)}')
        return True
        
    except FFRuntimeError as e:
        # 读取 stderr 日志以获取详细错误信息
        error_details = ""
        if stderr_log_path and os.path.exists(stderr_log_path):
            try:
                with open(stderr_log_path, 'r', encoding='utf-8', errors='ignore') as f:
                    stderr_content = f.read()
                    if stderr_content:
                        # 提取最后几行错误信息
                        lines = stderr_content.strip().split('\n')
                        error_details = '\n'.join(lines[-5:]) if len(lines) > 5 else stderr_content
            except Exception:
                pass
        
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
        for ts_file in ts_files:
            final_filename = str(ts_file).replace('.ts', '.' + CONTAINER)
            if convert_ts_file(str(ts_file), final_filename, logger):
                success_count += 1
        
        if success_count > 0:
            logger.info(f'Converted {success_count} legacy .ts file(s) to {CONTAINER} format')
    
    # 在后台线程中运行，不阻塞主程序
    thread = Thread(target=convert_all, daemon=True)
    thread.start()
    return thread
