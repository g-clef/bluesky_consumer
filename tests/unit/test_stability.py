"""
Stability tests for the storage worker.

RED phase: these tests are written to fail against the current implementation
and pass once the fixes are applied.
"""
import asyncio
import time
import pytest
from unittest.mock import MagicMock, patch

import storage_worker.storage_worker as sw

# Must match the constant added to storage_worker.py in the fix.
MAX_FLUSH_FAILURES = 5

_MODULE = 'storage_worker.storage_worker'


def make_worker():
    """Return a StorageWorker with all external dependencies mocked out."""
    with patch(f'{_MODULE}.EventConsumer'), \
         patch(f'{_MODULE}.S3Writer'), \
         patch(f'{_MODULE}.HealthServer'):
        worker = sw.StorageWorker()
    # worker.consumer, worker.writer, worker.health_server are now MagicMocks.
    return worker


@pytest.mark.asyncio
async def test_flush_does_not_block_event_loop():
    """
    A slow flush must not starve the asyncio event loop.

    The test schedules a concurrent async task alongside the process loop.
    If flush() blocks the event loop (current bug), the concurrent task cannot
    run until after flush() returns, so its completion time will be >= flush
    end time.  After the fix (flush runs in a thread pool), the concurrent task
    runs during the flush and completes well before flush() finishes.
    """
    FLUSH_DURATION = 0.5
    MARGIN = 0.3  # concurrent task must finish at least MARGIN seconds before flush ends

    flush_done_at = None
    concurrent_done_at = None

    def slow_flush():
        nonlocal flush_done_at
        time.sleep(FLUSH_DURATION)          # simulates a slow S3 write
        flush_done_at = time.monotonic()
        return True

    async def concurrent_task():
        nonlocal concurrent_done_at
        await asyncio.sleep(0.05)           # small delay; in fixed code this runs during flush
        concurrent_done_at = time.monotonic()

    worker = make_worker()
    worker.consumer.poll_message = MagicMock(return_value=None)
    worker.consumer.commit = MagicMock()
    worker.writer.flush = slow_flush
    worker.writer.should_flush = MagicMock(return_value=False)
    worker.writer.get_buffer_size = MagicMock(return_value=1)
    worker.last_flush_time = time.time() - 10_000   # force time-based flush immediately
    worker.running = True

    concurrent = asyncio.create_task(concurrent_task())
    process = asyncio.create_task(worker.process())

    await asyncio.sleep(FLUSH_DURATION + 0.3)   # wait long enough for one flush cycle
    worker.running = False
    process.cancel()
    try:
        await process
    except asyncio.CancelledError:
        pass
    await concurrent

    assert flush_done_at is not None, "Flush never ran — check test setup"
    assert concurrent_done_at is not None, "Concurrent task never ran — check test setup"

    gap = flush_done_at - concurrent_done_at
    assert gap > MARGIN, (
        f"Event loop was blocked during flush: concurrent task finished only "
        f"{gap:.3f}s before flush ended (expected > {MARGIN}s). "
        "Fix: run flush() via run_in_executor so the event loop stays free."
    )


@pytest.mark.asyncio
async def test_worker_stops_after_repeated_flush_failures():
    """
    The worker must self-terminate after MAX_FLUSH_FAILURES consecutive failures.

    When flush() returns False repeatedly the buffer grows without bound and
    the worker is effectively stuck.  The fix adds a failure counter: after
    MAX_FLUSH_FAILURES the worker sets self.running = False so Kubernetes can
    restart it with a clean state.
    """
    worker = make_worker()
    worker.consumer.poll_message = MagicMock(return_value=None)
    worker.consumer.commit = MagicMock()
    worker.writer.flush = MagicMock(return_value=False)   # always fails
    worker.writer.should_flush = MagicMock(return_value=False)
    worker.writer.get_buffer_size = MagicMock(return_value=1)
    worker.last_flush_time = time.time() - 10_000   # force time-based flush immediately
    worker.running = True

    process = asyncio.create_task(worker.process())

    # Give the loop plenty of time to accumulate MAX_FLUSH_FAILURES failures.
    await asyncio.sleep(1.0)

    assert not worker.running, (
        f"Worker should have set running=False after {MAX_FLUSH_FAILURES} "
        "consecutive flush failures, but it is still running."
    )
    assert process.done(), (
        "Process loop should have exited after setting running=False."
    )

    # Clean up if somehow still running.
    if not process.done():
        process.cancel()
        try:
            await process
        except asyncio.CancelledError:
            pass
