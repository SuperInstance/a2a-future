"""Tests for Future, Promise, FutureExecutor, combinators, and AsyncChannel."""

import asyncio
import pytest
from a2a_future import (
    Future, FutureState, FutureError, FutureCancelled,
    Promise, FutureExecutor, AsyncChannel,
)
from a2a_future.combinator import all, race, any, all_settled, SettleOutcome


# ── Future tests ────────────────────────────────────────────────────────────

class TestFuture:
    def test_initial_state(self):
        f = Future()
        assert f.state is FutureState.PENDING
        assert not f.is_done

    def test_resolve(self):
        f = Future()
        f.resolve(42)
        assert f.state is FutureState.RESOLVED
        assert f.value == 42
        assert f.is_done

    def test_reject(self):
        f = Future()
        err = RuntimeError("boom")
        f.reject(err)
        assert f.state is FutureState.REJECTED
        assert f.error is err

    def test_cancel(self):
        f = Future()
        f.cancel("nope")
        assert f.state is FutureState.CANCELLED

    def test_cannot_resolve_twice(self):
        f = Future()
        f.resolve(1)
        with pytest.raises(FutureError):
            f.resolve(2)

    def test_cannot_reject_after_resolve(self):
        f = Future()
        f.resolve(1)
        with pytest.raises(FutureError):
            f.reject(RuntimeError("nope"))

    def test_value_before_resolve_raises(self):
        f = Future()
        with pytest.raises(FutureError):
            _ = f.value

    @pytest.mark.asyncio
    async def test_await_resolved(self):
        f = Future()
        f.resolve("hello")
        assert await f == "hello"

    @pytest.mark.asyncio
    async def test_await_rejected(self):
        f = Future()
        f.reject(ValueError("bad"))
        with pytest.raises(ValueError, match="bad"):
            await f

    @pytest.mark.asyncio
    async def test_await_cancelled(self):
        f = Future()
        f.cancel("stopped")
        with pytest.raises(FutureCancelled):
            await f

    @pytest.mark.asyncio
    async def test_await_blocks_until_resolved(self):
        f = Future()

        async def resolve_later():
            await asyncio.sleep(0.05)
            f.resolve(99)

        asyncio.ensure_future(resolve_later())
        result = await asyncio.wait_for(f, timeout=1.0)
        assert result == 99

    def test_on_cancel_callback(self):
        f = Future()
        called = []
        f.on_cancel(lambda: called.append(True))
        f.cancel()
        assert called == [True]

    def test_repr(self):
        f = Future()
        assert "pending" in repr(f)
        f.resolve(42)
        assert "42" in repr(f)


# ── Promise tests ───────────────────────────────────────────────────────────

class TestPromise:
    @pytest.mark.asyncio
    async def test_basic_resolve(self):
        p = Promise[int]()
        p.resolve(10)
        val = await p.future
        assert val == 10

    @pytest.mark.asyncio
    async def test_reject(self):
        p = Promise()
        p.reject(RuntimeError("err"))
        with pytest.raises(RuntimeError, match="err"):
            await p.future

    @pytest.mark.asyncio
    async def test_then_chain(self):
        p = Promise[int]()
        result = []

        p.then(lambda v: v * 3).then(lambda v: result.append(v))
        p.resolve(7)

        await asyncio.sleep(0.1)
        assert result == [21]

    @pytest.mark.asyncio
    async def test_catch(self):
        p = Promise()
        result = []

        p.catch(lambda e: result.append(str(e)))
        p.reject(ValueError("oops"))

        await asyncio.sleep(0.1)
        assert result == ["oops"]

    @pytest.mark.asyncio
    async def test_finally(self):
        p = Promise()
        called = []

        p.finally_(lambda: called.append("done"))
        p.resolve(1)

        await asyncio.sleep(0.1)
        assert called == ["done"]

    @pytest.mark.asyncio
    async def test_then_error_propagates_to_catch(self):
        p = Promise[int]()
        result = []

        p.then(lambda v: 1 / 0).catch(lambda e: result.append(type(e).__name__))
        p.resolve(1)

        await asyncio.sleep(0.1)
        assert result == ["ZeroDivisionError"]


# ── FutureExecutor tests ────────────────────────────────────────────────────

class TestFutureExecutor:
    @pytest.mark.asyncio
    async def test_submit_and_gather(self):
        ex = FutureExecutor()
        f = ex.submit(lambda: self._slow(42))
        results = await ex.gather()
        assert results == [42]
        await ex.shutdown()

    @pytest.mark.asyncio
    async def test_timeout_rejects(self):
        ex = FutureExecutor()
        f = ex.submit(lambda: asyncio.sleep(10), timeout=0.05)
        with pytest.raises(asyncio.TimeoutError):
            await f
        await ex.shutdown()

    @pytest.mark.asyncio
    async def test_concurrency_limit(self):
        ex = FutureExecutor(max_concurrency=2)
        running = 0
        max_running = 0

        async def job():
            nonlocal running, max_running
            running += 1
            max_running = max(max_running, running)
            await asyncio.sleep(0.1)
            running -= 1
            return True

        futures = [ex.submit(job) for _ in range(5)]
        for f in futures:
            await f
        assert max_running <= 2
        await ex.shutdown()

    @pytest.mark.asyncio
    async def test_shutdown_cancels_pending(self):
        ex = FutureExecutor()
        f = ex.submit(lambda: asyncio.sleep(10))
        await ex.shutdown(cancel_pending=True)
        assert f.state is FutureState.CANCELLED

    async def _slow(self, val):
        await asyncio.sleep(0.01)
        return val


# ── Combinator tests ────────────────────────────────────────────────────────

class TestCombinators:
    @pytest.mark.asyncio
    async def test_all_success(self):
        f1, f2, f3 = Future(), Future(), Future()
        f1.resolve(1)
        f2.resolve(2)
        f3.resolve(3)
        result = await all([f1, f2, f3])
        assert result == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_all_rejects_on_first_error(self):
        f1, f2 = Future(), Future()
        f1.resolve(1)
        f2.reject(RuntimeError("fail"))
        with pytest.raises(RuntimeError):
            await all([f1, f2])

    @pytest.mark.asyncio
    async def test_race_first_resolve(self):
        f1, f2 = Future(), Future()
        f1.resolve("fast")
        # f2 stays pending
        result = await race([f1, f2])
        assert result == "fast"

    @pytest.mark.asyncio
    async def test_race_first_reject(self):
        f1, f2 = Future(), Future()
        f1.reject(RuntimeError("nope"))
        with pytest.raises(RuntimeError):
            await race([f1, f2])

    @pytest.mark.asyncio
    async def test_race_empty_raises(self):
        with pytest.raises(ValueError):
            await race([])

    @pytest.mark.asyncio
    async def test_any_first_success(self):
        f1, f2 = Future(), Future()
        f1.reject(RuntimeError("bad"))
        f2.resolve("good")
        result = await any([f1, f2])
        assert result == "good"

    @pytest.mark.asyncio
    async def test_any_all_fail(self):
        f1, f2 = Future(), Future()
        f1.reject(RuntimeError("a"))
        f2.reject(RuntimeError("b"))
        with pytest.raises(RuntimeError, match="all futures rejected"):
            await any([f1, f2])

    @pytest.mark.asyncio
    async def test_all_settled(self):
        f1, f2, f3 = Future(), Future(), Future()
        f1.resolve(1)
        f2.reject(RuntimeError("err"))
        f3.resolve(3)
        results = await all_settled([f1, f2, f3])
        assert len(results) == 3
        assert results[0].outcome is SettleOutcome.FULFILLED
        assert results[0].value == 1
        assert results[1].outcome is SettleOutcome.REJECTED
        assert results[2].outcome is SettleOutcome.FULFILLED


# ── AsyncChannel tests ──────────────────────────────────────────────────────

class TestAsyncChannel:
    @pytest.mark.asyncio
    async def test_send_recv(self):
        ch = AsyncChannel[int]()
        await ch.send(1)
        assert await ch.recv() == 1

    @pytest.mark.asyncio
    async def test_fifo_order(self):
        ch = AsyncChannel()
        for i in range(5):
            await ch.send(i)
        for i in range(5):
            assert await ch.recv() == i

    @pytest.mark.asyncio
    async def test_close_terminates_recv(self):
        ch = AsyncChannel()
        ch.close()
        with pytest.raises(Exception):
            await ch.recv()

    @pytest.mark.asyncio
    async def test_send_after_close_raises(self):
        ch = AsyncChannel()
        ch.close()
        with pytest.raises(Exception):
            await ch.send(1)

    @pytest.mark.asyncio
    async def test_bounded_channel_blocks(self):
        ch = AsyncChannel(maxsize=2)
        await ch.send(1)
        await ch.send(2)
        # third send should block until something is consumed
        send_task = asyncio.ensure_future(ch.send(3))
        await asyncio.sleep(0.05)
        assert not send_task.done()
        await ch.recv()  # free one slot
        await asyncio.wait_for(send_task, timeout=0.5)
        assert ch.size == 2

    def test_send_nowait_full_raises(self):
        ch = AsyncChannel(maxsize=1)
        ch.send_nowait(1)
        from a2a_future.channel import ChannelFull
        with pytest.raises(ChannelFull):
            ch.send_nowait(2)

    @pytest.mark.asyncio
    async def test_async_iteration(self):
        ch = AsyncChannel()
        values = []

        async def producer():
            for i in range(3):
                await ch.send(i)
            ch.close()

        async def consumer():
            async for v in ch:
                values.append(v)

        await asyncio.gather(producer(), consumer())
        assert values == [0, 1, 2]
