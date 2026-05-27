"""Combinators — all, race, any, all_settled for collections of Futures."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import Enum
from typing import Any, Sequence

from .future import Future, FutureState


class SettleOutcome(str, Enum):
    FULFILLED = "fulfilled"
    REJECTED = "rejected"


@dataclass
class SettledResult:
    outcome: SettleOutcome
    value: Any = None
    error: BaseException | None = None


async def all(futures: Sequence[Future]) -> list:
    """Resolve when *all* futures resolve; reject on first error."""
    results: list = []
    for f in futures:
        results.append(await f)
    return results


async def race(futures: Sequence[Future]) -> Any:
    """Resolve/reject with the first future to settle."""
    if not futures:
        raise ValueError("race() requires at least one Future")

    # Check for already-settled futures first
    for f in futures:
        if f.is_done:
            if f.state is FutureState.CANCELLED:
                raise f.error  # type: ignore[misc]
            if f.state is FutureState.REJECTED:
                raise f.error  # type: ignore[misc]
            return f._value

    async def _wait_one(fut: Future) -> Future:
        await fut._event.wait()
        return fut

    tasks = [asyncio.ensure_future(_wait_one(f)) for f in futures]
    done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    for t in tasks:
        t.cancel()
    winning_future = next(iter(done)).result()
    if winning_future.state is FutureState.CANCELLED:
        raise winning_future.error  # type: ignore[misc]
    if winning_future.state is FutureState.REJECTED:
        raise winning_future.error  # type: ignore[misc]
    return winning_future._value


async def any(futures: Sequence[Future]) -> Any:
    """Resolve with the first successful future; reject only if all fail."""
    from builtins import any as any_f

    if not futures:
        raise ValueError("any() requires at least one Future")
    errors: list[BaseException] = []
    resolved_event = asyncio.Event()
    resolved_value: Any = None

    async def _watch(fut: Future) -> None:
        nonlocal resolved_value
        try:
            val = await fut
            if not resolved_event.is_set():
                resolved_value = val
                resolved_event.set()
        except Exception as exc:
            errors.append(exc)
            if len(errors) == len(futures) and not resolved_event.is_set():
                resolved_event.set()

    tasks = [asyncio.ensure_future(_watch(f)) for f in futures]
    await resolved_event.wait()
    for t in tasks:
        t.cancel()
    has_resolved = any_f(f.state is FutureState.RESOLVED for f in futures)
    if resolved_event.is_set() and has_resolved:
        return resolved_value
    raise RuntimeError("all futures rejected") from errors[0]


async def all_settled(futures: Sequence[Future]) -> list[SettledResult]:
    """Wait for all futures to settle, returning outcome records."""
    results: list[SettledResult] = []
    for f in futures:
        try:
            val = await f
            results.append(SettledResult(outcome=SettleOutcome.FULFILLED, value=val))
        except Exception as exc:
            results.append(SettledResult(outcome=SettleOutcome.REJECTED, error=exc))
    return results
