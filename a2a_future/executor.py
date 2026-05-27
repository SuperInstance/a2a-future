"""FutureExecutor — manage concurrent futures with timeouts and concurrency limits."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine, Generic, TypeVar

from .future import Future, FutureError, FutureState

T = TypeVar("T")


@dataclass
class FutureExecutor:
    """Submit coroutines and manage their lifecycle.

    Features:
      - Optional concurrency limit (semaphore)
      - Per-future timeouts
      - Graceful shutdown (cancel pending)
    """

    max_concurrency: int | None = None
    _semaphore: asyncio.Semaphore | None = field(default=None, init=False, repr=False)
    _futures: list[Future] = field(default_factory=list, init=False, repr=False)
    _tasks: list[asyncio.Task] = field(default_factory=list, init=False, repr=False)

    def _get_semaphore(self) -> asyncio.Semaphore | None:
        if self._semaphore is None and self.max_concurrency is not None:
            self._semaphore = asyncio.Semaphore(self.max_concurrency)
        return self._semaphore

    def submit(
        self,
        coro_factory: Callable[[], Coroutine[Any, Any, T]],
        timeout: float | None = None,
    ) -> Future[T]:
        """Submit a coroutine factory for execution. Returns a Future.

        The factory is called once execution begins (respecting concurrency limits).
        If *timeout* is set, the Future is rejected with TimeoutError on expiry.
        """
        fut: Future[T] = Future()
        self._futures.append(fut)

        async def _run() -> None:
            sem = self._get_semaphore()
            try:
                if sem is not None:
                    await sem.acquire()
                coro = coro_factory()
                try:
                    if timeout is not None:
                        result = await asyncio.wait_for(coro, timeout=timeout)
                    else:
                        result = await coro
                    fut.resolve(result)
                except Exception as exc:
                    if not fut.is_done:
                        fut.reject(exc)
            except Exception as exc:
                if not fut.is_done:
                    fut.reject(exc)
            finally:
                if sem is not None:
                    sem.release()

        task = asyncio.ensure_future(_run())
        self._tasks.append(task)
        return fut

    async def gather(self) -> list[Any]:
        """Await all submitted futures and return their values.

        Raises the first error encountered (mirrors asyncio.gather behaviour).
        """
        results = []
        for f in list(self._futures):
            results.append(await f)
        return results

    async def shutdown(self, cancel_pending: bool = True) -> None:
        """Shut down the executor.

        If *cancel_pending*, all still-pending futures are cancelled and
        their backing tasks are cancelled.
        """
        if cancel_pending:
            for f in list(self._futures):
                if f.state is FutureState.PENDING:
                    try:
                        f.cancel("executor shutdown")
                    except FutureError:
                        pass
            for t in list(self._tasks):
                if not t.done():
                    t.cancel()
        # wait for tasks to finish
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        self._futures.clear()

    @property
    def pending_count(self) -> int:
        return sum(1 for f in self._futures if f.state is FutureState.PENDING)

    @property
    def done_count(self) -> int:
        return sum(1 for f in self._futures if f.is_done)
