import m3u8
import os
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

# Segments smaller than this are discarded (no real content); avoids ~262B empty/minimal MP4s
MIN_SEGMENT_SIZE = 64 * 1024  # 64 KiB

# TS input options: force mpegts, probe more so variable packet size (188/192/204) still yields streams
HLS_TS_INPUT_OPTS = '-f mpegts -probesize 10M -analyzeduration 10M -scan_all_pmts 1'

# Standard MPEG-TS packet size (sync 0x47 + 187 bytes). HLS chunks may be 188/192/204 mixed.
TS_PACKET_SIZE = 188


def _normalize_ts_to_188(data):
    """Extract 188-byte TS packets from data that may have 188/192/204 byte packets. Returns bytes."""
    if not data or len(data) < TS_PACKET_SIZE:
        return b''
    out = []
    i = 0
    while i + TS_PACKET_SIZE <= len(data):
        if data[i] == 0x47:  # sync byte
            out.append(data[i:i + TS_PACKET_SIZE])
            # Advance by detected packet size (188, 192, or 204); index must be < len(data)
            if i + 188 < len(data) and data[i + 188] == 0x47:
                i += 188
            elif i + 192 < len(data) and data[i + 192] == 0x47:
                i += 192
            elif i + 204 < len(data) and data[i + 204] == 0x47:
                i += 204
            else:
                i += 188
        else:
            i += 1
    return b''.join(out)


def _ts_pid(packet):
    """Return PID of a 188-byte TS packet."""
    if len(packet) < 3:
        return -1
    return ((packet[1] & 0x1F) << 8) | packet[2]


def _ensure_pat_pmt_at_start(data):
    """Reorder TS packets so PAT (PID 0) and PMT appear at the start. FFmpeg needs them to detect streams."""
    if not data or len(data) < TS_PACKET_SIZE:
        return data
    packets = [data[i:i + TS_PACKET_SIZE] for i in range(0, len(data), TS_PACKET_SIZE) if data[i] == 0x47]
    if not packets:
        return data
    pat_packets = [p for p in packets if _ts_pid(p) == 0x0000]
    if not pat_packets:
        return data  # no PAT in segment, cannot fix
    # PMT PID from PAT or default 0x0100
    pmt_pid = 0x0100
    for p in pat_packets:
        if len(p) < 18:
            continue
        # payload_start_indicator is in byte 1, bit 6; payload starts at byte 4 when no adaptation
        if (p[1] & 0x40) == 0:
            continue
        ptr = p[4]  # pointer_field is first byte of payload
        off = 5 + ptr  # PAT section starts after pointer_field
        if off + 12 <= len(p) and p[off] == 0x00:  # table_id PAT
            pmt_pid = ((p[off + 10] & 0x1F) << 8) | p[off + 11]
            break
    pmt_packets = [p for p in packets if _ts_pid(p) == pmt_pid]
    pat_pids = {0x0000, pmt_pid}
    rest = [p for p in packets if _ts_pid(p) not in pat_pids]
    reordered = pat_packets + pmt_packets + rest
    return b''.join(reordered) if reordered else data


def getVideoNativeHLS(self, url, filename, m3u_processor=None, file_original=None):
    if file_original is None:
        file_original = filename
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
    current_original_mp4 = file_original  # desired final name for current segment
    segment_files = []  # (ts_path, safe_mp4_path, original_mp4_path) for completed segments

    if segment_during_download:
        # Use the initial filename for first segment (but as .ts)
        current_file = filename[:-len('.' + CONTAINER)] + '.ts'
        current_file_size = 0

    def execute():
        nonlocal error, current_file, current_file_size, current_original_mp4, segment_files
        downloaded_list = []
        outfile = [None]  # Use list to allow modification in nested function
        last_success = time.monotonic()

        def within_grace():
            return (time.monotonic() - last_success) <= float(HLS_TRANSIENT_GRACE_SECONDS)

        def try_refresh_url():
            """On bitrate/playlist change, get fresh playlist URL and continue same recording."""
            try:
                new_url = self.getVideoUrl()
                if new_url:
                    url_ref[0] = new_url
                    self.logger.info('Refreshed playlist URL (stream may have changed bitrate/variants)')
                    return True
            except Exception as e:
                self.logger.debug('Refresh playlist URL failed: %s', e)
            return False
        
        def convert_segment_to_mp4(ts_file_path, final_safe_filename, final_original_filename=None):
            """Convert a .ts segment file to final format (MP4) in background thread; optionally rename to original name."""
            if final_original_filename is None:
                final_original_filename = final_safe_filename

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
                    
                    stdout = open(final_safe_filename + '.postprocess_stdout.log', 'w+') if DEBUG else subprocess.DEVNULL
                    stderr = open(final_safe_filename + '.postprocess_stderr.log', 'w+') if DEBUG else subprocess.DEVNULL
                    output_str = '-c:a copy -c:v copy'
                    ff = FFmpeg(executable=FFMPEG_PATH, inputs={ts_file_path: HLS_TS_INPUT_OPTS}, outputs={final_safe_filename: output_str})
                    ff.run(stdout=stdout, stderr=stderr)
                    os.remove(ts_file_path)
                    # Rename to original filename if different (e.g. special chars / emoji in title)
                    if final_original_filename != final_safe_filename and os.path.exists(final_safe_filename):
                        try:
                            os.rename(final_safe_filename, final_original_filename)
                            for ext in ('.postprocess_stdout.log', '.postprocess_stderr.log'):
                                p = final_safe_filename + ext
                                if os.path.exists(p):
                                    try:
                                        os.remove(p)
                                    except OSError:
                                        pass
                        except OSError as e:
                            self.logger.warning(f'Could not rename to original filename: {e}')
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
            nonlocal current_file, current_file_size, current_original_mp4, segment_files
            if segment_during_download:
                # Check if we need to start a new segment
                if current_file_size >= int(segment_size_bytes):
                    if outfile[0] and not outfile[0].closed:
                        outfile[0].flush()
                        outfile[0].close()
                    
                    # Convert the completed segment to MP4 (like pause/resume logic)
                    prev_ts_file = current_file
                    prev_final_safe = prev_ts_file.replace('.ts', '.' + CONTAINER)
                    if os.path.exists(prev_ts_file):
                        prev_sz = os.path.getsize(prev_ts_file)
                        if prev_sz >= MIN_SEGMENT_SIZE:
                            segment_files.append((prev_ts_file, prev_final_safe, current_original_mp4))
                            convert_segment_to_mp4(prev_ts_file, prev_final_safe, current_original_mp4)
                        elif prev_sz > 0:
                            try:
                                os.remove(prev_ts_file)
                            except OSError:
                                pass
                    
                    # Generate new filename with timestamp (like pause/resume)
                    new_result = self.genOutFilename(create_dir=True)
                    if isinstance(new_result, tuple):
                        new_safe, new_original = new_result
                    else:
                        new_safe = new_original = new_result
                    current_file = new_safe[:-len('.' + CONTAINER)] + '.ts'
                    current_original_mp4 = new_original
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
                    content = m3u_processor(content)
                chunklist = m3u8.loads(content)
                if len(chunklist.segments) == 0:
                    # Sometimes live playlists temporarily return empty; tolerate within grace.
                    if within_grace():
                        if try_refresh_url():
                            pass
                        sleep(float(HLS_RETRY_SLEEP_SECONDS))
                        continue
                    return
                for chunk in chunklist.segment_map + chunklist.segments:
                    if chunk.uri in downloaded_list:
                        continue
                    did_download = True
                    downloaded_list.append(chunk.uri)
                    chunk_uri = chunk.uri
                    self.debug('Downloading ' + chunk_uri)
                    if not chunk_uri.startswith("https://"):
                        chunk_uri = '/'.join(url_ref[0].split('.m3u8')[0].split('/')[:-1]) + '/' + chunk_uri
                    try:
                        m = session.get(chunk_uri, headers=self.headers, cookies=self.cookies, timeout=30)
                    except Exception as e:
                        if within_grace():
                            downloaded_list.pop()  # retry this chunk next time
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
                            downloaded_list.pop()
                            if try_refresh_url():
                                break
                            self.logger.warning('HLS chunk status %s (transient, retrying)', m.status_code)
                            sleep(float(HLS_RETRY_SLEEP_SECONDS))
                            break
                        return
                    file_handle = get_output_file()
                    # Normalize to 188-byte TS packets so FFmpeg can detect streams (avoids mixed 188/192/204)
                    chunk_data = _normalize_ts_to_188(m.content)
                    if chunk_data:
                        file_handle.write(chunk_data)
                        last_success = time.monotonic()
                        if segment_during_download:
                            current_file_size += len(chunk_data)
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
                final_safe = current_file.replace('.ts', '.' + CONTAINER)
                final_original = current_original_mp4
                stderr_path = final_safe + '.postprocess_stderr.log'
                stderr_file = None
                try:
                    # Always write stderr log so it can be inspected when conversion fails (e.g. exit 254)
                    stdout = open(final_safe + '.postprocess_stdout.log', 'w+') if DEBUG else subprocess.DEVNULL
                    stderr_file = open(stderr_path, 'w+')
                    output_str = '-c:a copy -c:v copy'
                    ff = FFmpeg(executable=FFMPEG_PATH, inputs={current_file: HLS_TS_INPUT_OPTS}, outputs={final_safe: output_str})
                    ff.run(stdout=stdout, stderr=stderr_file)
                    stderr_file.close()
                    stderr_file = None
                    try:
                        os.remove(stderr_path)
                    except OSError:
                        pass
                    os.remove(current_file)
                    if final_original != final_safe and os.path.exists(final_safe):
                        try:
                            os.rename(final_safe, final_original)
                            for ext in ('.postprocess_stdout.log', '.postprocess_stderr.log'):
                                p = final_safe + ext
                                if os.path.exists(p):
                                    try:
                                        os.remove(p)
                                    except OSError:
                                        pass
                        except OSError as e:
                            self.logger.warning(f'Could not rename final segment to original filename: {e}')
                except FFRuntimeError as e:
                    if stderr_file:
                        try:
                            stderr_file.close()
                        except OSError:
                            pass
                    if e.exit_code and e.exit_code != 255:
                        self.logger.error(f'Error converting final segment: {e}. Check {stderr_path!r} for ffmpeg stderr.')
                    # Fallback: normalize TS to 188-byte packets, put PAT/PMT at start, then retry
                    norm_ts = current_file + '.norm.ts'
                    fallback_ok = False
                    try:
                        with open(current_file, 'rb') as f:
                            raw = f.read()
                        norm_data = _normalize_ts_to_188(raw)
                        if len(norm_data) >= MIN_SEGMENT_SIZE:
                            # FFmpeg needs PAT/PMT at start to detect streams (segment may start mid-stream on cut)
                            norm_data = _ensure_pat_pmt_at_start(norm_data)
                            if len(norm_data) >= MIN_SEGMENT_SIZE:
                                with open(norm_ts, 'wb') as f:
                                    f.write(norm_data)
                                devnull = subprocess.DEVNULL
                                out_opts = '-c:a copy -c:v copy'
                                # Try 1: with forced mpegts + probe options
                                try:
                                    ff2 = FFmpeg(executable=FFMPEG_PATH, inputs={norm_ts: HLS_TS_INPUT_OPTS}, outputs={final_safe: out_opts})
                                    ff2.run(stdout=devnull, stderr=devnull)
                                    fallback_ok = True
                                except FFRuntimeError:
                                    # Try 2: no -f mpegts, let FFmpeg auto-detect (can help when content is fMP4 or demuxer is picky)
                                    try:
                                        ff3 = FFmpeg(executable=FFMPEG_PATH, inputs={norm_ts: '-probesize 20M -analyzeduration 20M'}, outputs={final_safe: out_opts})
                                        ff3.run(stdout=devnull, stderr=devnull)
                                        fallback_ok = True
                                    except FFRuntimeError:
                                        pass
                                if fallback_ok:
                                    os.remove(norm_ts)
                                    os.remove(current_file)
                                    if final_original != final_safe and os.path.exists(final_safe):
                                        try:
                                            os.rename(final_safe, final_original)
                                        except OSError:
                                            pass
                                    if os.path.exists(stderr_path):
                                        try:
                                            os.remove(stderr_path)
                                        except OSError:
                                            pass
                                    self.logger.info('Final segment converted after normalizing TS (fallback).')
                    except Exception:
                        pass
                    if not fallback_ok:
                        if os.path.exists(norm_ts):
                            try:
                                os.remove(norm_ts)
                            except OSError:
                                pass
                        self.logger.warning(f'TS file kept (conversion failed): {current_file!r}. You can convert it manually.')
                except Exception as e:
                    if stderr_file:
                        try:
                            stderr_file.close()
                        except OSError:
                            pass
                    self.logger.error(f'Unexpected error converting final segment: {e}. Check {stderr_path!r} for ffmpeg stderr.')
                    self.logger.warning(f'TS file kept: {current_file!r}. You can convert it manually.')
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
            ff = FFmpeg(executable=FFMPEG_PATH, inputs={tmpfilename: HLS_TS_INPUT_OPTS}, outputs={filename: output_str})
            ff.run(stdout=stdout, stderr=stderr)
            os.remove(tmpfilename)
            if file_original != filename and os.path.exists(filename):
                try:
                    os.rename(filename, file_original)
                    for ext in ('.postprocess_stdout.log', '.postprocess_stderr.log'):
                        p = filename + ext
                        if os.path.exists(p):
                            try:
                                os.remove(p)
                            except OSError:
                                pass
                except OSError as e:
                    self.logger.warning(f'Could not rename to original filename: {e}')
        except FFRuntimeError as e:
            if e.exit_code and e.exit_code != 255:
                return False

    return True
