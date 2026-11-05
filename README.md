# celery-bittensor
Bittensor's Schedule for Celery

## Installation

```
pip install celery-bittensor
```

Requires Python 3.10+.


## Example

```python
import celery
import celery_bittensor

app = celery.Celery()


@app.on_after_configure.connect
def setup_periodic_tasks(sender: celery.Celery, **kwargs):
    # Calls test('hello') every 10 finalized blocks.
    sender.add_periodic_task(
        celery_bittensor.schedule(run_every=10),
        test.s("hello"),
        name="add every 10",
    )

    # Calls test('hello') every 10 finalized blocks relative to block number 0.
    sender.add_periodic_task(
        celery_bittensor.schedule(run_every=10, relative=True),
        test.s("hello"),
        name="add every 10 relative to 0",
    )

    # Calls test('hello') every 10 new blocks on testnet.
    sender.add_periodic_task(
        celery_bittensor.schedule(run_every=10, finalized=False, network="test"),
        test.s("hello"),
        name="add every 10 on testnet",
    )

    # Calls test('hello') at first 10 blocks each epoch in subnet 1
    sender.add_periodic_task(
        celery_bittensor.epoch("0-9", netuid=1),
        test.s("hello"),
        name="first 10 block each epoch",
    )

    # Calls test('hello') at 50th and 75th block each epoch in subnet 1
    sender.add_periodic_task(
        celery_bittensor.epoch("49,74", netuid=1),
        test.s("hello"),
        name="50 and 75 block each epoch",
    )


@app.task
def test(arg):
    print(arg)
```

Bittensor tasks may be rescheduled (awaiting connection, etc.).
Please run Beat with the celery_bittensor Scheduler:

```
celery -A tasks beat --scheduler celery_bittensor.PersistentScheduler
```

## License

BSD 3-Clause License, see [LICENSE](LICENSE).
