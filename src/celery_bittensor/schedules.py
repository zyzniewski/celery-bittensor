import atexit
import datetime
import queue
import threading
import time
from collections.abc import Callable
from typing import ClassVar

import celery.schedules

from .client import BittensorSynced, Block

STOP_TIMEOUT = datetime.timedelta(seconds=5)


class BaseBittensorSchedule(celery.schedules.BaseSchedule):
    _lock: ClassVar[threading.Lock] = threading.Lock()
    _clients: ClassVar[dict[tuple[str, bool], BittensorSynced]] = {}

    def __init__(
        self,
        network: str = "finney",
        finalized: bool = True,
        backlog: int = 0,
        interval: datetime.timedelta | float = datetime.timedelta(seconds=1.0),
        nowfun: Callable | None = None,
        app: celery.Celery | None = None,
    ):
        super().__init__(nowfun, app)

        if not isinstance(interval, datetime.timedelta):
            interval = datetime.timedelta(seconds=interval)

        self.network = network
        self.finalized = finalized
        self.interval = interval
        self.last_run_at = Block(0, self.maybe_make_aware(datetime.datetime.min))

        self._bittensor: BittensorSynced | None = None
        self._blocks = queue.Queue[Block](maxsize=backlog)

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
            "backlog": self._blocks.maxsize,
            "finalized": self.finalized,
            "interval": self.interval,
            "last_run_at": self.last_run_at,
            "network": self.network,
            "nowfun": self.nowfun,
        }

    def __setstate__(self, state: dict) -> None:
        state.setdefault("interval", datetime.timedelta(seconds=1.0))

        backlog = state.pop("backlog", 0)

        self.__dict__.update(state)
        self._app = None
        self._bittensor = None
        self._blocks = queue.Queue[Block](maxsize=backlog)

    @property
    def bittensor(self) -> BittensorSynced:
        if not self._bittensor:
            with BaseBittensorSchedule._lock:
                if (self.network, self.finalized) not in BaseBittensorSchedule._clients:
                    BaseBittensorSchedule._clients[(self.network, self.finalized)] = (
                        BittensorSynced(
                            network=self.network,
                            finalized=self.finalized,
                        )
                    )
                    BaseBittensorSchedule._clients[
                        (self.network, self.finalized)
                    ].start()

                self._bittensor = BaseBittensorSchedule._clients[
                    (self.network, self.finalized)
                ]
                self._bittensor.blocks.subscribe(self._blocks)

        return self._bittensor

    def is_due(self, last_run_at: datetime.datetime) -> celery.schedules.schedstate:
        if self._blocks.empty() and not self.bittensor.connected:
            return celery.schedules.schedstate(
                is_due=False, next=self.interval.total_seconds()
            )

        try:
            block = self._blocks.get_nowait()
        except queue.Empty:
            if self.last_run_at.number == 0:
                return celery.schedules.schedstate(
                    is_due=False, next=self.interval.total_seconds()
                )

            block = self.last_run_at

        if self.last_run_at.number == 0:
            self.last_run_at = block

        is_due = self._prev_block(block.number) > self.last_run_at.number

        if is_due:
            self.last_run_at = block

        if not self._blocks.empty():
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
        if not self.bittensor.connected:
            return self.interval

        assert self.bittensor.block_time

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


def _stop_clients() -> None:
    for client in BaseBittensorSchedule._clients.values():
        client.stop()

    deadline = time.monotonic() + STOP_TIMEOUT.total_seconds()

    for client in BaseBittensorSchedule._clients.values():
        client.join(max(deadline - time.monotonic(), 0))


atexit.register(_stop_clients)
