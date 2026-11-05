import heapq

import celery.beat
import celery.schedules

from .schedules import BaseBittensorSchedule


class LazyBlockNumber(celery.beat.BeatLazyFunc):
    def __init__(self, entry=None):
        super().__init__(
            self._block_number,
            entry,
        )

    def __reduce__(self):
        return (self.__class__, (None,))

    def _block_number(self, entry):
        if not entry or not entry.block:
            return None

        return entry.block.number


class Scheduler(celery.beat.Scheduler):
    class Entry(celery.beat.ScheduleEntry):
        def __init__(self, *args, block=None, **kwargs):
            super().__init__(*args, **kwargs)

            self.block = None
            self.args = [
                LazyBlockNumber(self) if isinstance(arg, LazyBlockNumber) else arg
                for arg in self.args
            ]
            self.kwargs = {
                key: LazyBlockNumber(self)
                if isinstance(value, LazyBlockNumber)
                else value
                for key, value in self.kwargs.items()
            }

        def is_due(self):
            if not isinstance(self.schedule, BaseBittensorSchedule):
                return super().is_due()

            is_due, next_time, self.block = self.schedule.is_due_block(self.last_run_at)

            return celery.schedules.schedstate(
                is_due,
                next_time,
            )

    def tick(
        self,
        event_t=celery.beat.event_t,
        min=min,
        heappop=heapq.heappop,
        heappush=heapq.heappush,
    ):
        interval = super().tick(event_t, min, heappop, heappush)

        if not interval:
            return interval

        if not self._heap:
            return interval

        _, priority, entry = heappop(self._heap)

        heappush(
            self._heap,
            event_t(
                self._when(entry, interval),
                priority,
                entry,
            ),
        )

        timestamp, _, entry = self._heap[0]
        interval = timestamp - self._when(entry, 0)

        return min(max(interval, 0), self.max_interval)


class PersistentScheduler(Scheduler, celery.beat.PersistentScheduler):
    pass
