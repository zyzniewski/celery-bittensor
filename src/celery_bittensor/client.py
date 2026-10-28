import asyncio
import contextlib
import datetime
import logging
import threading
from typing import NamedTuple

logger = logging.getLogger(__name__)


class Block(NamedTuple):
    number: int
    timestamp: datetime.datetime


class BittensorSynced:
    RETRY = datetime.timedelta(seconds=5)

    def __init__(
        self,
        network: str,
        finalized: bool,
    ):
        self.network = network
        self.finalized = finalized

        self.block: Block | None = None
        self.block_time: float | None = None

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
                self.block = None

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

            async for block in client.blocks(finalized=self.finalized):
                logger.info("New block received on %s: %d", self.network, block.number)

                self.block = Block(
                    block.number,
                    datetime.datetime.now(datetime.timezone.utc),
                )
