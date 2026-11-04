import asyncio
import threading


class LoopThread:
    def __init__(self, name="celery-bittensor"):
        self._thread = threading.Thread(
            daemon=True,
            name=name,
            target=self._run,
        )
        self._loop = None

        self._started = threading.Event()
        self._shutdown = asyncio.Event()

    def call(self, *args, **kwargs):
        if not self._loop:
            raise RuntimeError("Event loop is not running")

        return self._loop.call_soon_threadsafe(*args, **kwargs)

    def join(self, timeout=None):
        self._thread.join(timeout)

    def start(self):
        self._thread.start()
        self._started.wait()

    def stop(self):
        if not self._thread.is_alive():
            return

        if not self._loop:
            raise RuntimeError("Event loop is not running")

        self._loop.call_soon_threadsafe(self._shutdown.set)

    def submit(self, coro):
        if not self._loop:
            raise RuntimeError("Event loop is not running")

        return asyncio.run_coroutine_threadsafe(coro, self._loop)

    def _run(self):
        try:
            asyncio.run(self._wait())
        finally:
            self._loop = None

    async def _wait(self):
        self._loop = asyncio.get_running_loop()
        self._started.set()

        await self._shutdown.wait()
