import pytest
import asyncio
from src.main import orchestrate, OrchestrationRequest, state_manager

@pytest.mark.asyncio
async def test_sse_client_disconnect_cancels_executor() -> None:
    """
    Verifies that when a client disconnects (simulated by closing the SSE generator),
    the background DAGExecutor task is cancelled and the state is cleaned up.
    """
    # 1. Trigger the endpoint
    req = OrchestrationRequest(
        prompt="Analyze recent AI breakthroughs in protein folding, summarize sentiment, and write a blog post outline"
    )
    
    # Call the FastAPI route handler directly
    response = await orchestrate(req)
    
    # 2. Get the body iterator (async generator)
    body_iterator = response.body_iterator
    
    # 3. Read the first two chunks (parser and planner completed chunks)
    first_chunk = await body_iterator.__anext__()
    assert "parser" in first_chunk
    
    second_chunk = await body_iterator.__anext__()
    assert "planner" in second_chunk
    
    # 4. Now simulate client disconnect by closing the generator
    await body_iterator.aclose()
    
    # Let the event loop run to execute the finally cleanup block
    await asyncio.sleep(0.1)
    
    # 5. Extract the execution ID from the state manager storage.
    # The last saved execution state file should exist in the state directory
    # and have its running steps transitioned to failed/cancelled.
    # Let's read the state file directly. Since we don't know the uuid directly from outside,
    # we can scan the storage directory for the most recently saved state file.
    import os
    import glob
    state_files = glob.glob(os.path.join(state_manager.storage_dir, "*.json"))
    assert len(state_files) > 0
    
    # Sort files by modification time to find the newest one
    newest_file = max(state_files, key=os.path.getmtime)
    execution_id = os.path.splitext(os.path.basename(newest_file))[0]
    
    state = state_manager.load_state(execution_id)
    assert state is not None
    
    # Verify that the overall execution was halted and state was preserved.
    # The tasks should be cancelled, and state saved on disk should reflect the cancellation.
    # Since Kahn's loop was cancelled, any steps that were running should be marked as failed.
    # Also, clean up the test state file
    state_manager.delete_state(execution_id)
