# Developer Debugging Log & Battle Scars

This log details the actual, unpolished issues and errors I encountered while building this orchestrator from scratch. Mostly 3 AM head-scratchers.

---

## Issue 1: Event Loop Hell in pytest-asyncio
**Symptom**:
```
RuntimeError: Task <Task pending name='Task-2' ...> got Future <Future pending> attached to a different loop
```
**Struggle**:
I initialized the `TokenBucketRateLimiter` and `ManualBatcher` as global instances in `main.py` and instantiated their `asyncio.Lock` and `asyncio.Event` primitives inside `__init__`. 
This works fine when running the uvicorn server because there is only one global event loop. But `pytest` creates a fresh, separate event loop for *each* test case. So when a test imported `main.py`, the locks/events got bound to the first test loop. Subsequent tests crashed because they tried to await locks from the first loop that was already closed.

**Fix**:
Instead of instantiating locks/events once in `__init__`, I refactored the accessors to lazily bind locks to the active loop on addition/acquisition:
```python
current_loop = asyncio.get_running_loop()
if not hasattr(self, "_loop") or self._loop != current_loop:
    self._loop = current_loop
    self.lock = asyncio.Lock()
    # rebuild events/futures...
```
This is slightly hacky but it 100% fixed the pytest loop mismatch crashes.

---

## Issue 2: Manual Batcher Flusher Race Conditions
**Symptom**:
Under high concurrency load, the manual batcher flusher background task would randomly exit or multiple flushers would get spawned, leaving some futures hanging forever.

**Struggle**:
I originally wrote the background flusher check like this:
```python
if not self.background_task:
    self.background_task = asyncio.create_task(self._background_flusher())
```
But `add()` is called concurrently from multiple async tasks. Since this check wasn't inside the lock, multiple tasks entered the block before the first one could assign `self.background_task`. This spawned duplicate flusher loops, leading to race conditions where one flusher cleared the buffer but the other flusher threw errors or left futures un-triggered.

**Fix**:
Moved the flusher creation block inside the `async with self.lock` critical section:
```python
async with self.lock:
    # ...
    if self.background_task is None or self.background_task.done():
        self.background_task = asyncio.create_task(self._background_flusher())
```
This guarantees that exactly one flusher loop runs at any given time.

---

## Issue 3: SSE Disconnect Task Leaks
**Symptom**:
Evaluating the FastAPI endpoints showed that if a client connected to the SSE stream and disconnected early (e.g. closing their browser tab), the background DAGExecutor task would continue running in the background until it timed out or successfully completed, wasting tokens and system budget.

**Struggle**:
FastAPI/Uvicorn closes the connection, but they don't automatically cancel the background tasks spawned inside the endpoint unless you explicitly catch the disconnect.

**Fix**:
Wrapped the SSE generator loop inside a `try...finally` block. In the `finally` section, we verify if the background task is done. If not, we explicitly call `.cancel()` and await it so that resources are freed up immediately.
```python
finally:
    if not task.done():
        logger.warn("SSE connection closed while task was running. Cancelling task.")
        task.cancel()
        try:
            await asyncio.wait_for(task, timeout=2.0)
        except Exception:
            pass
```
Tested this by running a client script and killing it mid-run. The server log now prints `SSE connection closed while task was running. Cancelling task.` and halts the DAG immediately.
