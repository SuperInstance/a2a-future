"""AsyncChannel — streaming results between agents."""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

T = TypeVar("T")


class ChannelClosed(Exception):
    """Raised when operating on a closed channel."""


@dataclass
class AsyncChannel(Generic[T]):
    """An async queue for streaming values between producers and consumers.

    - Multiple producers can send values.
    - Multiple consumers can iterate.
    - Closing the channel signals end-of-stream.
    - Buffer size is optionally bounded; ``send`` blocks when full.
    """

    maxsize: int = 0  # 0 = unbounded
    _buffer: deque = field(default_factory=deque, init=False, repr=False)
    _closed: bool = field(default=False, init=False)
    _waiters: list[asyncio.Event] = field(default_factory=list, init=False)
    _send_waiters: list[asyncio.Event] = field(default_factory=list, init=False)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False)

    @property
    def is_closed(self) -> bool:
        return self._closed

    @property
    def size(self) -> int:
        return len(self._buffer)

    async def send(self, value: T) -> None:
        """Enqueue a value. Blocks if the buffer is full."""
        if self._closed:
            raise ChannelClosed("cannot send to a closed channel")

        while self.maxsize > 0 and len(self._buffer) >= self.maxsize:
            evt = asyncio.Event()
            self._send_waiters.append(evt)
            await evt.wait()
            self._send_waiters = [e for e in self._send_waiters if not e.is_set()]

        self._buffer.append(value)
        # wake one consumer
        if self._waiters:
            self._waiters.pop(0).set()

    def send_nowait(self, value: T) -> None:
        """Non-blocking send. Raises ChannelFull if the buffer is at capacity."""
        if self._closed:
            raise ChannelClosed("cannot send to a closed channel")
        if self.maxsize > 0 and len(self._buffer) >= self.maxsize:
            raise ChannelFull("channel buffer is full")
        self._buffer.append(value)
        if self._waiters:
            self._waiters.pop(0).set()

    async def recv(self) -> T:
        """Dequeue a value. Blocks if the buffer is empty.

        Raises ChannelClosed when the channel is closed and empty.
        """
        while not self._buffer:
            if self._closed:
                raise ChannelClosed("channel is closed and empty")
            evt = asyncio.Event()
            self._waiters.append(evt)
            await evt.wait()
            self._waiters = [e for e in self._waiters if not e.is_set()]

        value = self._buffer.popleft()
        # wake one sender
        if self._send_waiters:
            self._send_waiters.pop(0).set()
        return value

    def close(self) -> None:
        """Close the channel. Wakes all waiters."""
        self._closed = True
        for evt in self._waiters:
            evt.set()
        for evt in self._send_waiters:
            evt.set()

    def __aiter__(self):
        return self

    async def __anext__(self) -> T:
        try:
            return await self.recv()
        except ChannelClosed:
            raise StopAsyncIteration

    def __repr__(self) -> str:
        state = "closed" if self._closed else "open"
        return f"<AsyncChannel state={state} size={len(self._buffer)} maxsize={self.maxsize}>"


class ChannelFull(Exception):
    """Raised by send_nowait when the channel buffer is full."""
