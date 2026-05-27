# a2a-future

Async futures and promises for agent-to-agent communication.

## Installation

```bash
pip install a2a-future
```

## Usage

### Future

```python
from a2a_future import Future

f = Future()
f.resolve(42)
print(await f)  # 42
```

### Promise

```python
from a2a_future import Promise

p = Promise[int]()
p.then(lambda v: v * 2).then(lambda v: print(v))
p.resolve(21)  # prints 42
```

### Combinators

```python
from a2a_future import all, race, any, all_settled, Future

results = await all([f1, f2, f3])
first = await race([f1, f2])
```

### FutureExecutor

```python
from a2a_future import FutureExecutor

ex = FutureExecutor(max_concurrency=4)
fut = ex.submit(lambda: do_async_work(), timeout=5.0)
results = await ex.gather()
await ex.shutdown()
```

### AsyncChannel

```python
from a2a_future import AsyncChannel

ch = AsyncChannel[maxsize=10]()
async for item in ch:
    process(item)
```

## License

MIT
