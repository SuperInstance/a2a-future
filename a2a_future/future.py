"""Core Future class — an async-resolvable value with state tracking."""

from __future__ import annotations

import asyncio
import enum
from dataclasses import dataclass, field
from typing import Any, Callable, Generic, TypeVar

T = TypeVar("T")


class FutureState(enum.Enum):
    PENDING = "pending"
    RESOLVED = "resolved"
    REJECTED = "rejected"
    CANCELLED = "cancelled"


class FutureError(Exception):
    """Raised when a Future is used illegally."""


class FutureCancelled(FutureError):
    """Raised when a cancelled Future is awaited."""


@dataclass
class Future(Generic[T]):
    """An async-resolvable value for agent-to-agent communication.

    Supports pending → resolved / rejected / cancelled transitions.
    Awaiting a Future blocks until it settles; awaiting a cancelled Future
    raises FutureCancelled.
    """

    _state: FutureState = field(default=FutureState.PENDING, init=False)
    _value: Any = field(default=None, init=False)
    _error: BaseException | None = field(default=None, init=False)
    _event: asyncio.Event = field(default_factory=asyncio.Event, init=False)
    _cancel_callbacks: list[Callable[[], Any]] = field(default_factory=list, init=False)

    # --- properties -----------------------------------------------------------

    @property
    def state(self) -> FutureState:
        return self._state

    @property
    def value(self) -> T:
        if self._state is not FutureState.RESOLVED:
            raise FutureError(f"Value not available (state={self._state.value})")
        return self._value  # type: ignore[return-value]

    @property
    def error(self) -> BaseException:
        if self._state is not FutureState.REJECTED:
            raise FutureError(f"Error not available (state={self._state.value})")
        assert self._error is not None
        return self._error

    @property
    def is_done(self) -> bool:
        return self._state is not FutureState.PENDING

    # --- resolve / reject / cancel -------------------------------------------

    def resolve(self, value: T) -> None:
        if self._state is not FutureState.PENDING:
            raise FutureError(f"Cannot resolve (state={self._state.value})")
        self._value = value
        self._state = FutureState.RESOLVED
        self._event.set()

    def reject(self, error: BaseException) -> None:
        if self._state is not FutureState.PENDING:
            raise FutureError(f"Cannot reject (state={self._state.value})")
        self._error = error
        self._state = FutureState.REJECTED
        self._event.set()

    def cancel(self, reason: str = "cancelled") -> None:
        if self._state is not FutureState.PENDING:
            raise FutureError(f"Cannot cancel (state={self._state.value})")
        self._error = FutureCancelled(reason)
        self._state = FutureState.CANCELLED
        for cb in self._cancel_callbacks:
            try:
                cb()
            except Exception:
                pass
        self._event.set()

    def on_cancel(self, callback: Callable[[], Any]) -> None:
        """Register a callback fired on cancellation."""
        self._cancel_callbacks.append(callback)

    # --- await ----------------------------------------------------------------

    def __await__(self):  # type: ignore[override]
        return self._wait().__await__()

    async def _wait(self) -> T:
        await self._event.wait()
        if self._state is FutureState.CANCELLED:
            raise self._error  # type: ignore[misc]
        if self._state is FutureState.REJECTED:
            raise self._error  # type: ignore[misc]
        return self._value

    # --- helpers --------------------------------------------------------------

    def __repr__(self) -> str:
        extra = ""
        if self._state is FutureState.RESOLVED:
            extra = f", value={self._value!r}"
        elif self._state is FutureState.REJECTED:
            extra = f", error={self._error!r}"
        return f"<Future state={self._state.value}{extra}>"
