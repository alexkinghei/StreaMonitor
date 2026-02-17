import json
import os
import subprocess
import time
from threading import Thread
from websocket import create_connection, WebSocketConnectionClosedException, WebSocketException, WebSocketTimeoutException
from contextlib import closing
from ffmpy import FFmpeg, FFRuntimeError
from parameters import (
    DEBUG, CONTAINER, SEGMENT_SIZE, parse_segment_size, FFMPEG_PATH,
    HLS_TRANSIENT_GRACE_SECONDS, HLS_RETRY_SLEEP_SECONDS,
)


def getVideoWSSVR(self, url, filename):
    self.stopDownloadFlag = False
    error = False
    url = url.replace('fmp4s://', 'wss://')

    suffix = ''
    if hasattr(self, 'filename_extra_suffix'):
        suffix = self.filename_extra_suffix

    basefilename = filename[:-len('.' + CONTAINER)]
    filename = basefilename + suffix + '.' + CONTAINER
    tmpfilename = basefilename + '.tmp.mp4'

    def debug_(message):
        self.debug(message, filename + '.log')

    last_success = [time.monotonic()]

    def within_grace():
        return (time.monotonic() - last_success[0]) <= float(HLS_TRANSIENT_GRACE_SECONDS)

    def execute():
        nonlocal error
        with open(tmpfilename, 'wb') as outfile:
            while not self.stopDownloadFlag:
                if not within_grace():
                    debug_('WSS download exceeded transient grace window; stopping')
                    error = True
                    try:
                        outfile.flush()
                    except (OSError, ValueError):
                        pass
                    return
                try:
                    with closing(create_connection(url, timeout=10)) as conn:
                        conn.settimeout(10)
                        conn.send('{"url":"stream/hello","version":"0.0.1"}')
                        while not self.stopDownloadFlag:
                            t = conn.recv()
                            try:
                                tj = json.loads(t)
                                if 'url' in tj:
                                    if tj['url'] == 'stream/qual':
                                        conn.send('{"quality":"test","url":"stream/play","version":"0.0.1"}')
                                        debug_('Connection opened')
                                        break
                                if 'message' in tj:
                                    if tj['message'] == 'ping':
                                        debug_('Server is not ready or there was a change')
                                        error = True
                                        try:
                                            outfile.flush()
                                        except (OSError, ValueError):
                                            pass
                                        return
                            except:
                                debug_('Failed to open the connection')
                                error = True
                                try:
                                    outfile.flush()
                                except (OSError, ValueError):
                                    pass
                                return

                        while not self.stopDownloadFlag:
                            payload = conn.recv()
                            if payload:
                                outfile.write(payload)
                                last_success[0] = time.monotonic()
                except WebSocketConnectionClosedException:
                    debug_('WebSocket connection closed - try to continue')
                    time.sleep(float(HLS_RETRY_SLEEP_SECONDS))
                    continue
                except WebSocketTimeoutException:
                    if within_grace():
                        time.sleep(float(HLS_RETRY_SLEEP_SECONDS))
                        continue
                    debug_('WebSocket timed out beyond grace window')
                    error = True
                    try:
                        outfile.flush()
                    except (OSError, ValueError):
                        pass
                    continue
                except WebSocketException as wex:
                    debug_('Error when downloading')
                    debug_(wex)
                    error = True
                    try:
                        outfile.flush()
                    except (OSError, ValueError):
                        pass
                    return

    def terminate():
        self.stopDownloadFlag = True

    process = Thread(target=execute)
    process.start()
    self.stopDownload = terminate
    process.join()
    self.stopDownload = None

    if error:
        return False

    if not os.path.exists(tmpfilename) or os.path.getsize(tmpfilename) == 0:
        return False

    # Post-processing
    try:
        stdout = open(filename + '.postprocess_stdout.log', 'w+') if DEBUG else subprocess.DEVNULL
        stderr = open(filename + '.postprocess_stderr.log', 'w+') if DEBUG else subprocess.DEVNULL
        output_str = '-c:a copy -c:v copy'
        segment_size_bytes = parse_segment_size(SEGMENT_SIZE)
        if segment_size_bytes is not None:
            output_str += f' -f segment -reset_timestamps 1 -segment_size {segment_size_bytes}'
            filename = basefilename + '_%03d' + suffix + '.' + CONTAINER
        ff = FFmpeg(executable=FFMPEG_PATH, inputs={tmpfilename: '-ignore_editlist 1'}, outputs={filename: output_str})
        ff.run(stdout=stdout, stderr=stderr)
        os.remove(tmpfilename)
    except FFRuntimeError as e:
        try:
            if os.path.exists(filename):
                os.remove(filename)
        except OSError:
            pass
        try:
            subprocess.run(
                [
                    FFMPEG_PATH, '-y',
                    '-err_detect', 'ignore_err',
                    '-fflags', '+discardcorrupt+genpts',
                    '-i', tmpfilename,
                    '-c:a', 'copy',
                    '-c:v', 'copy',
                    filename,
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=True,
            )
            os.remove(tmpfilename)
        except Exception:
            if e.exit_code:
                self.logger.error('WSS final remux failed (exit_code=%s)', e.exit_code)
            return False

    return True
