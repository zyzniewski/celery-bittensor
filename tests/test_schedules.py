import datetime
import unittest.mock

import pytest

from celery_bittensor import schedule
from celery_bittensor.client import Block

BLOCK_TIME = datetime.timedelta(seconds=12)


@pytest.mark.parametrize(
    "scheduler, last_run_at, block_number, prev_block",
    [
        (schedule(5, False), 100, 100, 100),
        (schedule(5, False), 100, 101, 100),
        (schedule(5, False), 100, 102, 100),
        (schedule(5, False), 100, 103, 100),
        (schedule(5, False), 100, 104, 100),
        (schedule(5, False), 100, 105, 105),
        (schedule(5, False), 105, 106, 105),
        (schedule(5, True), 101, 101, 101),
        (schedule(5, True), 101, 102, 101),
        (schedule(5, True), 101, 103, 101),
        (schedule(5, True), 101, 104, 101),
        (schedule(5, True), 101, 105, 101),
        (schedule(5, True), 101, 106, 106),
        (schedule(5, True), 106, 107, 106),
    ],
)
def test_schedule_prev_block(scheduler, last_run_at, block_number, prev_block):
    scheduler.last_run_at = Block(last_run_at, datetime.datetime.now())
    scheduler._bittensor = unittest.mock.Mock(
        block=Block(block_number, datetime.datetime.now()),
        block_time=BLOCK_TIME.total_seconds(),
    )

    assert scheduler._prev_block(block_number) == prev_block


@pytest.mark.parametrize(
    "scheduler, last_run_at, block_number, next_block",
    [
        (schedule(5, False), 100, 100, 105),
        (schedule(5, False), 100, 101, 105),
        (schedule(5, False), 100, 102, 105),
        (schedule(5, False), 100, 103, 105),
        (schedule(5, False), 100, 104, 105),
        (schedule(5, False), 100, 105, 110),
        (schedule(5, False), 105, 106, 110),
        (schedule(5, True), 101, 101, 106),
        (schedule(5, True), 101, 102, 106),
        (schedule(5, True), 101, 103, 106),
        (schedule(5, True), 101, 104, 106),
        (schedule(5, True), 101, 105, 106),
        (schedule(5, True), 101, 106, 111),
        (schedule(5, True), 106, 107, 111),
    ],
)
def test_schedule_next_block(scheduler, last_run_at, block_number, next_block):
    scheduler.last_run_at = Block(last_run_at, datetime.datetime.now())
    scheduler._bittensor = unittest.mock.Mock(
        block=Block(block_number, datetime.datetime.now()),
        block_time=BLOCK_TIME.total_seconds(),
    )

    assert scheduler._next_block(block_number) == next_block


@pytest.mark.parametrize(
    "scheduler, last_run_at, block_number, is_due",
    [
        (schedule(5, False), 100, 100, False),
        (schedule(5, False), 100, 101, False),
        (schedule(5, False), 100, 102, False),
        (schedule(5, False), 100, 103, False),
        (schedule(5, False), 100, 104, False),
        (schedule(5, False), 100, 105, True),
        (schedule(5, False), 105, 106, False),
        (schedule(5, True), 101, 101, False),
        (schedule(5, True), 101, 102, False),
        (schedule(5, True), 101, 103, False),
        (schedule(5, True), 101, 104, False),
        (schedule(5, True), 101, 105, False),
        (schedule(5, True), 101, 106, True),
        (schedule(5, True), 106, 107, False),
    ],
)
def test_schedule_is_due(scheduler, last_run_at, block_number, is_due):
    now = datetime.datetime.now(datetime.timezone.utc)

    scheduler.last_run_at = Block(last_run_at, now)
    scheduler._bittensor = unittest.mock.Mock(
        block=Block(block_number, now),
        block_time=BLOCK_TIME.total_seconds(),
    )

    assert scheduler.is_due(now).is_due is is_due
