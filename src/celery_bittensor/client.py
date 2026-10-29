import asyncio
import contextlib
import datetime
import logging
import queue
import threading
from typing import NamedTuple

logger = logging.getLogger(__name__)


class Block(NamedTuple):
    number: int
    timestamp: datetime.datetime


class PubSub:
    def __init__(self):
        self._subscribers: list[queue.Queue] = []

    def publish(self, message):
        for subscriber in self._subscribers:
            try:
                subscriber.put_nowait(message)
            except queue.Full:
                logger.debug("Subscriber queue is full")

    def subscribe(self, queue: queue.Queue):
        self._subscribers = [*self._subscribers, queue]


class BittensorSynced:
    RETRY = datetime.timedelta(seconds=5)

    def __init__(
        self,
        network: str,
        finalized: bool,
    ):
        self.network = network
        self.finalized = finalized

        self.block_time: float | None = None
        self.blocks = PubSub()

        self._thread = threading.Thread(
            daemon=True,
            name=f"BittensorSynced[{self.network}]",
            target=self._run,
        )
        self._stop = threading.Event()

        self._loop: asyncio.AbstractEventLoop | None = None
        self._task: asyncio.Task | None = None

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

        loop, task = self._loop, self._task

        if loop and task:
            with contextlib.suppress(RuntimeError):
                loop.call_soon_threadsafe(task.cancel)

    def join(self) -> None:
        self._thread.join()

    def _run(self) -> None:
        try:
            asyncio.run(self._main())
        except asyncio.CancelledError:
            pass
        finally:
            self._loop = None
            self._task = None

    async def _main(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._task = asyncio.current_task()

        if self._stop.is_set():
            return

        while True:
            try:
                await self._blocks_generator()
            except Exception:
                self.block_time = None

                logger.exception(
                    "Error occured during processing on %s. Reconnecting in %ds",
                    self.network,
                    self.RETRY.total_seconds(),
                )

            await asyncio.sleep(self.RETRY.total_seconds())

    async def _blocks_generator(self) -> None:
        import bittensor

        logger.info("Connecting to %s", self.network)

        async with bittensor.Subtensor(
            network=self.network,
            retry_forever=True,
        ) as client:
            logger.info("Connected to %s. Checking block time", self.network)

            self.block_time = await client.block_time()

            logger.info("Awaiting for blocks on %s", self.network)

            async for block_header in client.blocks(finalized=self.finalized):
                logger.info("New block seen on %s: %d", self.network, block_header.number)

                block = Block(
                    block_header.number,
                    datetime.datetime.now(datetime.timezone.utc),
                )

                self.blocks.publish(block)
