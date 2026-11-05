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
app.conf.beat_scheduler = "celery_bittensor.PersistentScheduler"
app.conf.beat_schedule = {
    "add-every-finalized-block": {
        "task": "tasks.test",
        "schedule": celery_bittensor.schedule(
            run_every=1,
        ),
        "args": ("New Block",),
    },
    "add-every-10-finalized-blocks-relative-to-0": {
        "task": "tasks.test",
        "schedule": celery_bittensor.schedule(
            run_every=10,
            relative=True,
        ),
        "args": ("Every 10 Blocks",),
    },
    "add-every-block-on-testnet": {
        "task": "tasks.test",
        "schedule": celery_bittensor.schedule(
            run_every=1,
            network="test",
            finalized=False,
        ),
        "args": ("New Test Block",),
    },
    "add-first-10-epoch": {
        "task": "tasks.test",
        "schedule": celery_bittensor.epoch(
            block="0-9",
            netuid=1,
        ),
        "args": ("First 10 Blocks of Subnet 1 Epoch",),
    },
    "add-50-and-75-epoch": {
        "task": "tasks.test_block",
        "schedule": celery_bittensor.epoch(
            block="49,74",
            netuid=1,
        ),
        "args": ("50th and 75th Block of Subnet #1 Epoch",),
        "kwargs": {
            "block": celery_bittensor.LazyBlockNumber(),
        },
    },
}



@app.task
def test(arg):
    print(arg)


@app.task
def test_block(arg, block):
    print(f"Block {block}: {arg}")
```

Bittensor tasks may be rescheduled (awaiting connection, etc.).
Please run Beat with `celery_bittensor.PersistentScheduler`:

```
celery -A tasks beat --scheduler celery_bittensor.PersistentScheduler
```

## License

BSD 3-Clause License, see [LICENSE](LICENSE).
