"""Promise — then/catch/finally chaining on top of Future."""

from __future__ import annotations

import asyncio
from typing import Any, Callable, Generic, TypeVar

from .future import Future, FutureState

T = TypeVar("T")
U = TypeVar("U")


class Promise(Generic[T]):
    """A then-able wrapper around a Future.

    Usage::

        p = Promise[int]()
        p.future  # hand this to the producer

        p.then(lambda v: v * 2).then(lambda v: print(v))
        p.catch(lambda err: print(err))

        # later, the producer:
        p.resolve(21)
    """

    def __init__(self) -> None:
        self._future: Future[T] = Future()

    @property
    def future(self) -> Future[T]:
        return self._future

    # --- resolve / reject shortcuts ------------------------------------------

    def resolve(self, value: T) -> None:
        self._future.resolve(value)

    def reject(self, error: BaseException) -> None:
        self._future.reject(error)

    # --- chaining ------------------------------------------------------------

    def then(self, on_fulfilled: Callable[[T], Any]) -> "Promise":
        """Chain a callback on resolution. Returns a new Promise for further chaining."""
        next_p: Promise = Promise()

        async def _watch() -> None:
            try:
                value = await self._future
                result = on_fulfilled(value)
                if asyncio.iscoroutine(result):
                    result = await result
                next_p.resolve(result)
            except Exception as exc:
                next_p.reject(exc)

        asyncio.ensure_future(_watch())
        return next_p

    def catch(self, on_rejected: Callable[[BaseException], Any]) -> "Promise":
        """Chain an error handler. Returns a new Promise."""
        next_p: Promise = Promise()

        async def _watch() -> None:
            try:
                value = await self._future
                next_p.resolve(value)
            except Exception as exc:
                try:
                    result = on_rejected(exc)
                    if asyncio.iscoroutine(result):
                        result = await result
                    next_p.resolve(result)
                except Exception as inner:
                    next_p.reject(inner)

        asyncio.ensure_future(_watch())
        return next_p

    def finally_(self, on_finally: Callable[[], Any]) -> "Promise":
        """Run a callback regardless of outcome, passing through the value/error."""
        next_p: Promise = Promise()

        async def _watch() -> None:
            try:
                value = await self._future
            except Exception as exc:
                try:
                    result = on_finally()
                    if asyncio.iscoroutine(result):
                        await result
                except Exception as inner:
                    next_p.reject(inner)
                    return
                next_p.reject(exc)
                return

            try:
                result = on_finally()
                if asyncio.iscoroutine(result):
                    await result
                next_p.resolve(value)
            except Exception as inner:
                next_p.reject(inner)

        asyncio.ensure_future(_watch())
        return next_p

    def __repr__(self) -> str:
        return f"<Promise future={self._future!r}>"
