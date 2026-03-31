import m3u8
import os
import subprocess
from threading import Thread
from ffmpy import FFmpeg, FFRuntimeError
from time import sleep
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


def getVideoNativeHLS(self, url, filename, m3u_processor=None):
    self.stopDownloadFlag = False
    error = False
    session = requests.Session()

    def execute():
        nonlocal error
        downloaded_list = []
        did_download = False
        tmpfilename = None
        outfile = None
        try:
            while not self.stopDownloadFlag:
                r = session.get(url, headers=self.headers, cookies=self.cookies)
                content = r.content.decode("utf-8")
                if m3u_processor:
                    processed_content = m3u_processor(content)
                    if processed_content is not None:
                        content = processed_content
                chunklist = m3u8.loads(content)
                if len(chunklist.segments) == 0:
                    return

                if outfile is None:
                    uses_fmp4 = len(chunklist.segment_map) > 0 or any(
                        chunk.uri.endswith(('.m4s', '.mp4', '.cmfv', '.cmfa'))
                        for chunk in chunklist.segments
                    )
                    tmp_extension = '.tmp.mp4' if uses_fmp4 else '.tmp.ts'
                    tmpfilename = filename[:-len('.' + CONTAINER)] + tmp_extension
                    outfile = open(tmpfilename, 'wb')

                for chunk in chunklist.segment_map + chunklist.segments:
                    if chunk.uri in downloaded_list:
                        continue
                    did_download = True
                    downloaded_list.append(chunk.uri)
                    chunk_uri = chunk.uri
                    self.debug('Downloading ' + chunk_uri)
                    if not chunk_uri.startswith("https://"):
                        chunk_uri = '/'.join(url.split('.m3u8')[0].split('/')[:-1]) + '/' + chunk_uri
                    m = session.get(chunk_uri, headers=self.headers, cookies=self.cookies)
                    if m.status_code != 200:
                        return
                    outfile.write(m.content)
                    if self.stopDownloadFlag:
                        return
                if not did_download:
                    sleep(10)
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

    # Post-processing
    try:
        stdout = open(filename + '.postprocess_stdout.log', 'w+') if DEBUG else subprocess.DEVNULL
        stderr = open(filename + '.postprocess_stderr.log', 'w+') if DEBUG else subprocess.DEVNULL
        output_str = '-c:a copy -c:v copy'
        suffix = ''
        if SEGMENT_TIME is not None:
            output_str += f' -f segment -reset_timestamps 1 -segment_time {str(SEGMENT_TIME)}'
            if hasattr(self, 'filename_extra_suffix'):
                suffix = self.filename_extra_suffix
            filename = filename[:-len('.' + CONTAINER)] + '_%03d' + suffix + '.' + CONTAINER
        ff = FFmpeg(executable=FFMPEG_PATH, inputs={tmpfilename: None}, outputs={filename: output_str})
        ff.run(stdout=stdout, stderr=stderr)
        os.remove(tmpfilename)
    except FFRuntimeError as e:
        if e.exit_code and e.exit_code != 255:
            return False

    return True
