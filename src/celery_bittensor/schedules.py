import atexit
import datetime
import queue
import threading
from collections.abc import Callable, Iterable
from typing import ClassVar

import celery.schedules

from .client import (
    BittensorSubscriber,
    BittensorSynced,
    Block,
    Subnet,
)
from .thread import LoopThread

STOP_TIMEOUT = datetime.timedelta(seconds=5)


class BaseBittensorSchedule(celery.schedules.BaseSchedule):
    _clients: ClassVar[dict[tuple[str, bool], BittensorSynced]] = {}
    _lock: ClassVar[threading.Lock] = threading.Lock()
    _thread: ClassVar[LoopThread | None] = None

    def __init__(
        self,
        network: str = "finney",
        finalized: bool = True,
        backlog: int = -1,
        interval: datetime.timedelta | float = datetime.timedelta(seconds=1.0),
        nowfun: Callable | None = None,
        app: celery.Celery | None = None,
    ):
        super().__init__(nowfun, app)

        if not isinstance(interval, datetime.timedelta):
            interval = datetime.timedelta(seconds=interval)

        self.network = network
        self.finalized = finalized
        self.backlog = backlog
        self.interval = interval
        self.last_run_at = Block(0, self.maybe_make_aware(datetime.datetime.min))

        self._bittensor: BittensorSubscriber | None = None

    def __eq__(self, other: object) -> bool:
        if isinstance(other, BaseBittensorSchedule):
            return (
                self.network == other.network
                and self.finalized == other.finalized
                and self.interval == other.interval
                and super().__eq__(other)
            )

        return NotImplemented

    def __getstate__(self) -> dict:
        return {
            "backlog": self.backlog,
            "finalized": self.finalized,
            "interval": self.interval,
            "last_run_at": self.last_run_at,
            "network": self.network,
            "nowfun": self.nowfun,
        }

    def __setstate__(self, state: dict) -> None:
        state.setdefault("interval", datetime.timedelta(seconds=1.0))

        self.__dict__.update(state)
        self._app = None
        self._bittensor = None

    @classmethod
    def get_client(cls, network: str, finalized: bool) -> BittensorSynced:
        with cls._lock:
            if (network, finalized) not in cls._clients:
                if not cls._thread:
                    cls._thread = LoopThread()
                    cls._thread.start()

                client = BittensorSynced(network, finalized)

                cls._thread.submit(client.run())
                cls._clients[(network, finalized)] = client

            return cls._clients[(network, finalized)]

    @property
    def bittensor(self) -> BittensorSubscriber:
        if not self._bittensor:
            client = BaseBittensorSchedule.get_client(self.network, self.finalized)

            self._bittensor = BittensorSubscriber(self.backlog)
            self._thread.submit(self._bittensor.attach(client))

        return self._bittensor

    def is_due(self, last_run_at: datetime.datetime) -> celery.schedules.schedstate:
        if self.bittensor.blocks.empty() and not self.bittensor.connected:
            return celery.schedules.schedstate(
                is_due=False, next=self.interval.total_seconds()
            )

        try:
            block = self.bittensor.blocks.get_nowait()
        except queue.Empty:
            if self.last_run_at.number == 0:
                return celery.schedules.schedstate(
                    is_due=False, next=self.interval.total_seconds()
                )

            block = self.last_run_at
        else:
            self.bittensor.blocks.task_done()

        if self.last_run_at.number == 0:
            self.last_run_at = block

        prev_block = self._prev_block(block.number)
        is_due = prev_block == block.number and prev_block > self.last_run_at.number

        if is_due:
            self.last_run_at = block

        if not self.bittensor.blocks.empty():
            return celery.schedules.schedstate(
                is_due=is_due,
                next=0,
            )

        return celery.schedules.schedstate(
            is_due=is_due,
            next=self._time_until_next(block).total_seconds(),
        )

    def _next_block(self, block: int) -> int:
        raise NotImplementedError

    def _prev_block(self, block: int) -> int:
        raise NotImplementedError

    def _time_until_next(self, block: Block) -> datetime.timedelta:
        if not self.bittensor.block_time:
            return self.interval

        target = self._next_block(block.number)
        elapsed = self.maybe_make_aware(self.now()) - block.timestamp
        remaining = (target - block.number) * datetime.timedelta(
            seconds=self.bittensor.block_time,
        ) - elapsed

        return max(remaining, self.interval)


class schedule(BaseBittensorSchedule):
    def __init__(
        self,
        run_every: int,
        relative: bool = False,
        **kwargs,
    ):
        if not isinstance(run_every, int):
            raise TypeError("run_every must be an integer")

        if run_every <= 0:
            raise ValueError("run_every must be a positive integer")

        super().__init__(**kwargs)

        self.run_every = run_every
        self.relative = relative

    def __eq__(self, other: object) -> bool:
        if isinstance(other, schedule):
            return (
                self.run_every == other.run_every
                and self.relative == other.relative
                and super().__eq__(other)
            )

        return self.run_every == other

    def __getstate__(self) -> dict:
        state = super().__getstate__()
        state.update(
            {
                "run_every": self.run_every,
                "relative": self.relative,
            }
        )

        return state

    def _next_block(self, block: int) -> int:
        return self._prev_block(block) + self.run_every

    def _prev_block(self, block: int) -> int:
        if self.relative:
            return block - block % self.run_every

        base = self.last_run_at.number
        return base + (block - base) // self.run_every * self.run_every


class epoch(BaseBittensorSchedule):
    def __init__(
        self,
        block: celery.schedules.Cronspec = "*",
        netuid: int = 0,
        **kwargs,
    ):
        super().__init__(
            **kwargs,
        )

        self.block = block
        self.netuid = netuid

        self.subnet: Subnet | None = None
        self.blocks: list[int] = []

    @staticmethod
    def _expand_cronspec(
        cronspec: celery.schedules.Cronspec,
        tempo: int,
    ) -> list[int]:
        if isinstance(cronspec, int):
            result = {cronspec}
        elif isinstance(cronspec, str):
            result = celery.schedules.crontab_parser(tempo).parse(cronspec)
        elif isinstance(cronspec, set):
            result = cronspec
        elif isinstance(cronspec, Iterable):
            result = set(cronspec)  # type: ignore
        else:
            raise TypeError(
                celery.schedules.CRON_INVALID_TYPE.format(type=type(cronspec))
            )

        return sorted(number for number in result if 0 <= number < tempo)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, epoch):
            return (
                self.block == other.block
                and self.netuid == other.netuid
                and super().__eq__(other)
            )

        return NotImplemented

    def __getstate__(self) -> dict:
        state = super().__getstate__()
        state.update(
            {
                "block": self.block,
                "blocks": [],
                "netuid": self.netuid,
                "subnet": None,
            }
        )

        return state

    def is_due(self, last_run_at: datetime.datetime) -> celery.schedules.schedstate:
        # wait for subnet info
        try:
            subnet = self.bittensor.subnets[self.netuid]
        except KeyError:
            if self.bittensor.connected:
                self._thread.submit(self.bittensor.fetch_subnets())

            return celery.schedules.schedstate(
                False,
                self.interval.total_seconds(),
            )

        # regenerate blocks from cronspec for a given tempo
        if (not self.subnet or self.subnet.tempo != subnet.tempo) and isinstance(
            self.block,
            str,
        ):
            self.blocks = self._expand_cronspec(self.block, subnet.tempo)

        self.subnet = subnet

        if self.subnet.tempo == 0 or not self.blocks:
            return celery.schedules.schedstate(
                False,
                self.interval.total_seconds(),
            )

        return super().is_due(last_run_at)

    def _next_block(self, block: int) -> int:
        assert self.subnet

        start = (
            self.subnet.epoch
            + (block - self.subnet.epoch) // self.subnet.tempo * self.subnet.tempo
        )

        for offset in self.blocks:
            if start + offset > block:
                return start + offset

        return start + self.subnet.tempo + self.blocks[0]

    def _prev_block(self, block: int) -> int:
        assert self.subnet

        start = (
            self.subnet.epoch
            + (block - self.subnet.epoch) // self.subnet.tempo * self.subnet.tempo
        )

        for offset in reversed(self.blocks):
            if start + offset <= block:
                return start + offset

        return start - self.subnet.tempo + self.blocks[-1]


def _stop_clients() -> None:
    for client in BaseBittensorSchedule._clients.values():
        client.stop()

    BaseBittensorSchedule._clients.clear()

    if BaseBittensorSchedule._thread:
        BaseBittensorSchedule._thread.stop()
        BaseBittensorSchedule._thread.join(STOP_TIMEOUT.total_seconds())
        BaseBittensorSchedule._thread = None


atexit.register(_stop_clients)
