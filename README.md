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
    # Calls test('hello') every 10 blocks.
    sender.add_periodic_task(
        celery_bittensor.schedule(10), test.s("hello"), name="add every 10"
    )


@app.task
def test(arg):
    print(arg)
```

## License

BSD 3-Clause License, see [LICENSE](LICENSE).
