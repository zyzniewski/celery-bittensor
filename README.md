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


@app.task
def test(arg):
    print(arg)
```

## License

BSD 3-Clause License, see [LICENSE](LICENSE).
