import heapq

import celery.beat


class Scheduler(celery.beat.Scheduler):
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
