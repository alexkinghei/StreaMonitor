import time
import signal
from threading import Thread


class CleanExit:
    class DummyThread(Thread):
        def __init__(self):
            super().__init__()
            self._stop = False

        def run(self):
            while True:
                if self._stop:
                    return
                time.sleep(1)

        def stop(self):
            self._stop = True

    dummy_thread = DummyThread()

    def __init__(self, streamers):
        self.streamers = streamers
        if not self.dummy_thread.is_alive():
            self.dummy_thread.start()
            signal.signal(signal.SIGINT, self.clean_exit)
            signal.signal(signal.SIGTERM, self.clean_exit)
            signal.signal(signal.SIGABRT, self.clean_exit)

    def __call__(self, *args, **kwargs):
        self.clean_exit()

    def _log(self, msg):
        try:
            import streamonitor.log as log
            log.Logger("clean_exit").get_logger().info(msg)
        except Exception:
            print("[clean_exit]", msg)

    def clean_exit(self, _=None, __=None):
        self._log("Shutdown: stopping all recordings first (graceful)...")
        for streamer in self.streamers:
            streamer.stop(None, None, True)
        # Wait until no one is recording (stopDownload has been processed and files closed)
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline and any(getattr(s, "recording", False) for s in self.streamers):
            time.sleep(0.5)
        self._log("Waiting for all streamer threads to exit...")
        for streamer in self.streamers:
            while streamer.is_alive():
                time.sleep(1)
        self._log("All stopped, exiting.")
        self.dummy_thread.stop()
