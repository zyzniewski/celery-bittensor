import asyncio
import contextlib
import datetime
import logging
import queue
import threading
import weakref
from typing import NamedTuple

import bittensor

logger = logging.getLogger(__name__)


class Block(NamedTuple):
    number: int
    timestamp: datetime.datetime


class Subnet(NamedTuple):
    tempo: int
    epoch: int


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
        self.block: Block | None = None
        self.subnets: dict[int, Subnet] = {}

        self._blocks = weakref.WeakSet[asyncio.Queue]()
        self._client: bittensor.Client | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._stop = threading.Event()
        self._subnets = asyncio.Semaphore()
        self._task: asyncio.Task | None = None

    @property
    def blocks(self) -> asyncio.Queue[Block]:
        if self._stop.is_set():
            raise RuntimeError("Cannot subscribe to blocks after stop has been called.")

        q = asyncio.Queue[Block]()
        self._blocks = self._blocks.union([q])
        return q

    @property
    def connected(self) -> bool:
        return self._client is not None

    async def fetch_subnets(self) -> None:
        if not self._client:
            return

        if self._subnets.locked():
            return

        async with self._subnets:
            logger.info("Fetching subnets from %s", self.network)

            snapshot = await self._client.at()
            tempos, epochs = await asyncio.gather(
                snapshot.query_map(bittensor.storage.SubtensorModule.Tempo),
                snapshot.query_map(bittensor.storage.SubtensorModule.LastEpochBlock),
            )

            self.subnets = {
                netuid: Subnet(tempo, epoch)
                for (netuid, tempo), (_, epoch) in zip(tempos, epochs)
            }

    async def run(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._task = asyncio.current_task()

        try:
            while not self._stop.is_set():
                try:
                    await self._blocks_generator()
                except Exception:
                    logger.exception(
                        "Error occurred during processing on %s. Reconnecting in %ds",
                        self.network,
                        self.RETRY.total_seconds(),
                    )

                await asyncio.sleep(self.RETRY.total_seconds())
        finally:
            self._task = None

    def stop(self) -> None:
        self._stop.set()

        loop, task = self._loop, self._task

        if not loop or not task:
            return

        with contextlib.suppress(RuntimeError):
            loop.call_soon_threadsafe(task.cancel)

    async def _blocks_generator(self) -> None:
        logger.info("Connecting to %s", self.network)

        try:
            async with bittensor.Subtensor(
                network=self.network,
                retry_forever=True,
            ) as self._client:
                logger.info("Connected to %s. Checking block time", self.network)

                self.block_time = await self._client.block_time()

                logger.info("Awaiting for blocks on %s", self.network)

                async for block_header in self._client.blocks(finalized=self.finalized):
                    if self.block and self.block.number >= block_header.number:
                        continue

                    logger.info(
                        "New block seen on %s: %d",
                        self.network,
                        block_header.number,
                    )

                    self.block = Block(
                        block_header.number,
                        datetime.datetime.now(datetime.timezone.utc),
                    )

                    for q in self._blocks:
                        q.put_nowait(self.block)
        finally:
            self._client = None


class BittensorSubscriber:
    def __init__(
        self,
        backlog: int = -1,
    ):
        self.blocks = queue.Queue[Block](maxsize=backlog)

        self._client: BittensorSynced | None = None
        self._task: asyncio.Task | None = None

    @property
    def block_time(self) -> float | None:
        if not self._client:
            return None

        return self._client.block_time

    @property
    def connected(self) -> bool:
        if not self._client:
            return False

        return self._client.connected

    async def attach(self, client: BittensorSynced):
        self._client = client
        self._task = asyncio.create_task(self._watch())

    @property
    def subnets(self) -> dict[int, Subnet]:
        if not self._client:
            return {}

        return self._client.subnets

    async def fetch_subnets(self) -> None:
        if not self._client:
            return

        return await self._client.fetch_subnets()

    async def _watch(self):
        try:
            if not self._client:
                raise RuntimeError("Not attached to a Client")

            blocks = self._client.blocks

            while True:
                block = await blocks.get()

                try:
                    self.blocks.put_nowait(block)
                except queue.Full:
                    logger.warning("Queue is full, dropping Block %d", block.number)
                finally:
                    blocks.task_done()
        finally:
            self._task = None
