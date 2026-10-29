import atexit
import datetime
import threading
from collections.abc import Callable
from typing import ClassVar

import celery.schedules

from .client import BittensorSynced, Block


class BaseBittensorSchedule(celery.schedules.BaseSchedule):
    INIT_PULL = datetime.timedelta(microseconds=100)

    _lock: ClassVar[threading.Lock] = threading.Lock()
    _clients: ClassVar[dict[tuple[str, bool], BittensorSynced]] = {}

    def __init__(
        self,
        network: str = "finney",
        finalized: bool = True,
        nowfun: Callable | None = None,
        app: celery.Celery | None = None,
    ):
        super().__init__(nowfun, app)

        self.network = network
        self.finalized = finalized
        self.last_run_at = Block(0, self.maybe_make_aware(datetime.datetime.min))

        self._bittensor: BittensorSynced | None = None

    def __eq__(self, other: object) -> bool:
        if isinstance(other, BaseBittensorSchedule):
            return (
                self.network == other.network
                and self.finalized == other.finalized
                and super().__eq__(other)
            )

        return NotImplemented

    def __getstate__(self) -> dict:
        return {
            "finalized": self.finalized,
            "last_run_at": self.last_run_at,
            "network": self.network,
            "nowfun": self.nowfun,
        }

    def __setstate__(self, state: dict) -> None:
        self.__dict__.update(state)
        self._app = None
        self._bittensor = None

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

        return self._bittensor

    def is_due(self, last_run_at: datetime.datetime) -> celery.schedules.schedstate:
        if not self.bittensor.block_time:
            return celery.schedules.schedstate(
                is_due=False, next=self.INIT_PULL.total_seconds()
            )

        block = self.bittensor.block

        if not block:
            return celery.schedules.schedstate(
                is_due=False, next=self.INIT_PULL.total_seconds()
            )

        if self.last_run_at.number == 0:
            self.last_run_at = block

        is_due = self._prev_block(block.number) > self.last_run_at.number

        if is_due:
            self.last_run_at = block

        return celery.schedules.schedstate(
            is_due=is_due,
            next=self._time_until_next(block).total_seconds(),
        )

    def _next_block(self, block: int) -> int:
        raise NotImplementedError

    def _prev_block(self, block: int) -> int:
        raise NotImplementedError

    def _time_until_next(self, block: Block) -> datetime.timedelta:
        assert self.bittensor.block_time is not None

        target = self._next_block(block.number)
        elapsed = self.maybe_make_aware(self.now()) - block.timestamp
        remaining = (target - block.number) * datetime.timedelta(
            seconds=self.bittensor.block_time
        ) - elapsed

        return max(remaining, self.INIT_PULL)


class schedule(BaseBittensorSchedule):
    def __init__(
        self,
        run_every: int,
        relative: bool = False,
        network: str = "finney",
        finalized: bool = True,
        nowfun: Callable | None = None,
        app: celery.Celery | None = None,
    ):
        if not isinstance(run_every, int):
            raise TypeError("run_every must be an integer")

        if run_every <= 0:
            raise ValueError("run_every must be a positive integer")

        super().__init__(
            network,
            finalized,
            nowfun,
            app,
        )

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

        return block - block % self.run_every


def stop_watchers() -> None:
    for watcher in BaseBittensorSchedule._clients.values():
        watcher.stop()

    for watcher in BaseBittensorSchedule._clients.values():
        watcher.join()


atexit.register(stop_watchers)
