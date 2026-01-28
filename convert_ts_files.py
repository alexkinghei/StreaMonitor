#!/usr/bin/env python3
"""
工具脚本：转换遗留的 .ts 文件到 .mp4 格式

这个脚本会扫描下载目录，找到所有 .ts 文件并转换为配置的容器格式（默认 mp4）。
转换完成后会删除原始的 .ts 文件。
"""

import os
import sys
import subprocess
from pathlib import Path
from ffmpy import FFmpeg, FFRuntimeError
from parameters import DOWNLOADS_DIR, CONTAINER, FFMPEG_PATH, DEBUG


def convert_ts_to_mp4(ts_file_path, final_filename):
    """转换 .ts 文件到最终格式"""
    try:
        # 检查文件是否存在且不为空
        if not os.path.exists(ts_file_path):
            return False, "文件不存在"
        
        if os.path.getsize(ts_file_path) == 0:
            os.remove(ts_file_path)
            return False, "文件为空，已删除"
        
        # 如果目标文件已存在，跳过
        if os.path.exists(final_filename):
            print(f"  目标文件已存在，跳过: {final_filename}")
            os.remove(ts_file_path)
            return True, "目标文件已存在"
        
        print(f"  正在转换: {os.path.basename(ts_file_path)} -> {os.path.basename(final_filename)}")
        
        stdout = open(final_filename + '.postprocess_stdout.log', 'w+') if DEBUG else subprocess.DEVNULL
        stderr = open(final_filename + '.postprocess_stderr.log', 'w+') if DEBUG else subprocess.DEVNULL
        
        output_str = '-c:a copy -c:v copy'
        if CONTAINER == 'mp4':
            output_str += ' -movflags +faststart'
        
        ff = FFmpeg(
            executable=FFMPEG_PATH,
            inputs={ts_file_path: None},
            outputs={final_filename: output_str}
        )
        ff.run(stdout=stdout, stderr=stderr)
        
        # 转换成功后删除 .ts 文件
        os.remove(ts_file_path)
        print(f"  ✓ 转换成功: {os.path.basename(final_filename)}")
        return True, "转换成功"
        
    except FFRuntimeError as e:
        if e.exit_code and e.exit_code != 255:
            error_msg = f"FFmpeg 错误 (退出码 {e.exit_code}): {e}"
            print(f"  ✗ {error_msg}")
            return False, error_msg
        return False, "FFmpeg 错误"
    except Exception as e:
        error_msg = f"意外错误: {e}"
        print(f"  ✗ {error_msg}")
        return False, error_msg


def find_and_convert_ts_files():
    """查找并转换所有 .ts 文件"""
    downloads_path = Path(DOWNLOADS_DIR)
    
    if not downloads_path.exists():
        print(f"错误: 下载目录不存在: {DOWNLOADS_DIR}")
        return
    
    print(f"扫描目录: {DOWNLOADS_DIR}")
    print(f"目标格式: {CONTAINER}")
    print("-" * 60)
    
    ts_files = list(downloads_path.rglob("*.ts"))
    
    if not ts_files:
        print("未找到 .ts 文件")
        return
    
    print(f"找到 {len(ts_files)} 个 .ts 文件\n")
    
    success_count = 0
    fail_count = 0
    skip_count = 0
    
    for ts_file in ts_files:
        # 跳过临时文件
        if ts_file.name.endswith('.tmp.ts'):
            continue
        
        # 生成目标文件名
        final_filename = str(ts_file).replace('.ts', '.' + CONTAINER)
        
        print(f"处理: {ts_file.relative_to(downloads_path)}")
        
        success, message = convert_ts_to_mp4(str(ts_file), final_filename)
        
        if success:
            if "已存在" in message:
                skip_count += 1
            else:
                success_count += 1
        else:
            fail_count += 1
        
        print()
    
    print("-" * 60)
    print(f"转换完成:")
    print(f"  成功: {success_count}")
    print(f"  跳过: {skip_count}")
    print(f"  失败: {fail_count}")
    print(f"  总计: {len(ts_files)}")


if __name__ == "__main__":
    try:
        find_and_convert_ts_files()
    except KeyboardInterrupt:
        print("\n\n用户中断")
        sys.exit(1)
    except Exception as e:
        print(f"\n错误: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
