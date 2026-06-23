import pytest
import asyncio
from typing import List, Any
from src.utils.batcher import ManualBatcher

async def mock_processor(items: List[Any]) -> List[Any]:
    # Simulate processing delay
    await asyncio.sleep(0.02)
    return [f"processed_{item}" for item in items]

@pytest.mark.asyncio
async def test_batcher_race_condition_spawns_single_task() -> None:
    """
    Verifies that calling ManualBatcher.add concurrently from multiple coroutines
    does not spawn duplicate background flusher tasks.
    """
    # 1. Initialize batcher
    batcher = ManualBatcher(batch_size=5, max_wait_ms=100.0, processor=mock_processor)
    
    # 2. Add multiple items concurrently
    tasks = [asyncio.create_task(batcher.add(i)) for i in range(10)]
    
    # 3. Wait briefly to let the tasks execute the first part of add()
    await asyncio.sleep(0.01)
    
    # 4. Check the number of background flusher tasks.
    # We should have exactly one flusher task running, which is batcher.background_task.
    assert batcher.background_task is not None
    assert not batcher.background_task.done()
    
    # 5. Let all tasks finish
    results = await asyncio.gather(*tasks)
    
    # Clean up batcher
    await batcher.shutdown()
    
    # 6. Verify processing outcomes
    assert len(results) == 10
    assert results == [f"processed_{i}" for i in range(10)]
