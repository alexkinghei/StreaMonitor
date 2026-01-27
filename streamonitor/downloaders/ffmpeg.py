import errno
import os
import subprocess
import sys

import requests.cookies
from threading import Thread
from parameters import DEBUG, SEGMENT_SIZE, parse_segment_size, CONTAINER, FFMPEG_PATH, FFMPEG_READRATE


def getVideoFfmpeg(self, url, filename):
    cmd = [
        FFMPEG_PATH,
        '-user_agent', self.headers['User-Agent']
    ]

    if type(self.cookies) is requests.cookies.RequestsCookieJar:
        cookies_text = ''
        for cookie in self.cookies:
            cookies_text += cookie.name + "=" + cookie.value + "; path=" + cookie.path + '; domain=' + cookie.domain + '\n'
        if len(cookies_text) > 10:
            cookies_text = cookies_text[:-1]
        cmd.extend([
            '-cookies', cookies_text
        ])

    if FFMPEG_READRATE:
        cmd.extend(['-readrate', f'{FFMPEG_READRATE!s}'])

    cmd.extend([
        '-max_reload', '20',
        '-seg_max_retry', '20',
        '-m3u8_hold_counters', '20',
        '-i', url,
        '-c:a', 'copy',
        '-c:v', 'copy',
    ])

    suffix = ''
    if hasattr(self, 'filename_extra_suffix'):
        suffix = self.filename_extra_suffix

    segment_size_bytes = parse_segment_size(SEGMENT_SIZE)
    if segment_size_bytes is not None:
        username = filename.rsplit('-', maxsplit=2)[0]
        # Build output filename pattern
        output_pattern = f'{username}-%Y%m%d-%H%M%S{suffix}.{CONTAINER}'
        
        # For MP4 format, we need to use movflags for proper real-time segmentation
        if CONTAINER == 'mp4':
            # Use segment_format_options to pass movflags to the MP4 muxer
            # Note: segment_size may not split exactly at the specified size for live streams
            # as it needs to wait for keyframes. Actual segment size may be slightly larger.
            cmd.extend([
                '-f', 'segment',
                '-reset_timestamps', '1',
                '-segment_size', segment_size_bytes,
                '-segment_format', 'mp4',
                '-segment_format_options', 'movflags=+frag_keyframe+empty_moov+default_base_moof',
                '-strftime', '1',
                output_pattern
            ])
        else:
            # For other formats (mkv, etc.), use standard segment options
            cmd.extend([
                '-f', 'segment',
                '-reset_timestamps', '1',
                '-segment_size', segment_size_bytes,
                '-segment_format', CONTAINER,
                '-strftime', '1',
                output_pattern
            ])
    else:
        cmd.extend([
            os.path.splitext(filename)[0] + suffix + '.' + CONTAINER
        ])

    class _Stopper:
        def __init__(self):
            self.stop = False

        def pls_stop(self):
            self.stop = True

    stopping = _Stopper()
    error = False

    def execute():
        nonlocal error
        try:
            stderr = open(filename + '.stderr.log', 'w+') if DEBUG else subprocess.DEVNULL
            startupinfo = None
            if sys.platform == "win32":
                startupinfo = subprocess.STARTUPINFO()
                startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            process = subprocess.Popen(
                args=cmd, stdin=subprocess.PIPE, stderr=stderr, stdout=subprocess.DEVNULL, startupinfo=startupinfo)
        except OSError as e:
            if e.errno == errno.ENOENT:
                self.logger.error('FFMpeg executable not found!')
                error = True
                return
            else:
                self.logger.error("Got OSError, errno: " + str(e.errno))
                error = True
                return

        while process.poll() is None:
            if stopping.stop:
                process.communicate(b'q')
                break
            try:
                process.wait(1)
            except subprocess.TimeoutExpired:
                pass

        if process.returncode and process.returncode != 0 and process.returncode != 255:
            self.logger.error('The process exited with an error. Return code: ' + str(process.returncode))
            error = True
            return

    thread = Thread(target=execute)
    thread.start()
    self.stopDownload = lambda: stopping.pls_stop()
    thread.join()
    self.stopDownload = None
    return not error
