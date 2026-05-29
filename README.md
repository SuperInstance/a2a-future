# a2a-future

**Async futures and promises for agent-to-agent communication** — Future, Promise, combinators, and a concurrent executor. Pure Python.

## What This Gives You

- **`Future`** — resolve/await pattern for async agent results
- **`Promise`** — chainable `.then()` transformations
- **Combinators** — `all`, `race`, `any`, `all_settled`
- **`FutureExecutor`** — concurrent execution with configurable max concurrency and timeouts
- **`Channel`** — typed communication channels between agents

## Installation

```bash
pip install a2a-future
```

## Quick Start

```python
from a2a_future import Future, Promise, all, race, FutureExecutor

# Future
f = Future()
f.resolve(42)
result = await f  # 42

# Promise chains
p = Promise[int]()
p.then(lambda v: v * 2).then(lambda v: print(v))
p.resolve(21)  # prints 42

# Combinators
results = await all([f1, f2, f3])
first_done = await race([f1, f2])

# Executor
ex = FutureExecutor(max_concurrency=4)
fut = ex.submit(lambda: do_async_work(), timeout=5.0)
await ex.shutdown()
```

## Testing

```bash
pip install -e .
pytest
```

## How It Fits

Async primitives for the `a2a-protocol` ecosystem. Used by agents to coordinate asynchronous task completion across the fleet.

## License

MIT
