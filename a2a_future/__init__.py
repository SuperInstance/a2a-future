"""a2a-future: Async futures and promises for agent-to-agent communication."""

from .future import Future, FutureState, FutureError, FutureCancelled
from .promise import Promise
from .executor import FutureExecutor
from .combinator import all, race, any, all_settled
from .channel import AsyncChannel

__all__ = [
    "Future",
    "FutureState",
    "FutureError",
    "FutureCancelled",
    "Promise",
    "FutureExecutor",
    "AsyncChannel",
    "all",
    "race",
    "any",
    "all_settled",
]
