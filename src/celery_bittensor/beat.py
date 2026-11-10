import celery.beat
import celery.schedules

from .schedules import BaseBittensorSchedule


class LazyBlockNumber(celery.beat.BeatLazyFunc):
    def __init__(self, entry=None):
        super().__init__(
            self._block_number,
            entry,
        )

    def __eq__(self, other):
        return bool(isinstance(other, LazyBlockNumber))

    def __reduce__(self):
        return (self.__class__, (None,))

    def _block_number(self, entry):
        if not entry or not entry.block:
            return None

        return entry.block.number


class ScheduleEntry(celery.beat.ScheduleEntry):
    def __init__(self, *args, block=None, **kwargs):
        super().__init__(*args, **kwargs)

        self.block = None
        self._bind_lazy()

    def is_due(self):
        if not isinstance(self.schedule, BaseBittensorSchedule):
            return super().is_due()

        is_due, next_time, self.block = self.schedule.is_due_block(self.last_run_at)

        return celery.schedules.schedstate(
            is_due,
            next_time,
        )

    def update(self, other):
        super().update(other)

        self._bind_lazy()

    def _bind_lazy(self):
        self.args = [
            LazyBlockNumber(self) if isinstance(arg, LazyBlockNumber) else arg
            for arg in self.args
        ]
        self.kwargs = {
            key: LazyBlockNumber(self) if isinstance(value, LazyBlockNumber) else value
            for key, value in self.kwargs.items()
        }


class PersistentScheduler(celery.beat.PersistentScheduler):
    Entry = ScheduleEntry
