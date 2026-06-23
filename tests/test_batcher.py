import asyncio
import pytest
from src.utils.batcher import ManualBatcher

@pytest.mark.asyncio
async def test_batcher_size_flush() -> None:
    """Verifies that the batcher flushes immediately once batch_size is reached."""
    processed_batches = []
    
    async def mock_processor(items: list) -> list:
        processed_batches.append(items)
        return [f"res_{item}" for item in items]

    # Batch size 3
    batcher = ManualBatcher(batch_size=3, max_wait_ms=1000.0, processor=mock_processor)
    
    # Add 2 items (should not flush yet)
    fut1 = asyncio.create_task(batcher.add(1))
    fut2 = asyncio.create_task(batcher.add(2))
    await asyncio.sleep(0.05)
    assert len(processed_batches) == 0
    
    # Add 3rd item (triggers immediate flush)
    fut3 = asyncio.create_task(batcher.add(3))
    
    results = await asyncio.gather(fut1, fut2, fut3)
    await batcher.shutdown()
    
    assert len(processed_batches) == 1
    assert processed_batches[0] == [1, 2, 3]
    assert results == ["res_1", "res_2", "res_3"]

@pytest.mark.asyncio
async def test_batcher_timeout_flush() -> None:
    """Verifies that the batcher flushes on timeout when batch size is not met."""
    processed_batches = []
    
    async def mock_processor(items: list) -> list:
        processed_batches.append(items)
        return [f"res_{item}" for item in items]

    # Batch size 10, timeout 100ms
    batcher = ManualBatcher(batch_size=10, max_wait_ms=100.0, processor=mock_processor)
    
    fut = asyncio.create_task(batcher.add(42))
    
    # Wait for timeout
    await asyncio.sleep(0.2)
    
    result = await fut
    await batcher.shutdown()
    
    assert len(processed_batches) == 1
    assert processed_batches[0] == [42]
    assert result == "res_42"

@pytest.mark.asyncio
async def test_batcher_partial_failures() -> None:
    """
    REQUIREMENT: Show that a batch of 5 items where 2 fail still returns 
    3 successes + 2 structured errors.
    """
    async def mock_processor(items: list) -> list:
        results = []
        for item in items:
            if item in [2, 4]:
                results.append(ValueError(f"Simulated error for item {item}"))
            else:
                results.append(f"success_{item}")
        return results

    batcher = ManualBatcher(batch_size=5, max_wait_ms=1000.0, processor=mock_processor)
    
    # Add 5 items
    futures = [
        asyncio.create_task(batcher.add(i)) for i in [1, 2, 3, 4, 5]
    ]
    
    # Gather with return_exceptions=True so failures don't abort
    results = await asyncio.gather(*futures, return_exceptions=True)
    await batcher.shutdown()
    
    assert len(results) == 5
    assert results[0] == "success_1"
    assert isinstance(results[1], ValueError)
    assert str(results[1]) == "Simulated error for item 2"
    assert results[2] == "success_3"
    assert isinstance(results[3], ValueError)
    assert str(results[3]) == "Simulated error for item 4"
    assert results[4] == "success_5"
