# Post-Mortem: What went wrong & what we learned

This is an honest breakdown of the bottlenecks, design regrets, and hard trade-offs encountered while building this system. It was mostly written at 3 AM after dealing with event loop errors.

---

## 1. Concurrency Bottlenecks (The Event Loop Slag)

### The Friday night panic
We ran a test script simulating **500 concurrent requests** to the API. Within 10 seconds, the event loop latency spiked from 2ms to over **850ms**, and memory usage climbed by **150MB** until the Uvicorn process became unresponsive. 

### Why did this happen?
- **Blocking File I/O**: The local `StateManager` ([state_manager.py](file:///Users/adib/Desktop/Agentic%20Ai%20system/multi-agent-orchestrator/src/failure/state_manager.py#L20-L28)) was saving states via `with open(..., "w") as f:` synchronously. Because filesystem operations in Python are blocking, calling this repeatedly for 500 tasks completely stalled the single-threaded asyncio event loop.
- **Unbounded Queues**: The FastAPI execution queue was unbounded. If the client stopped consuming data, the executor kept pushing chunks, causing the queue to grow. This leaked memory and eventually caused a crash.

### How we mitigated it
We added bounded limits (`maxsize=100`) to the queue to enforce backpressure. If we ever deploy to production, we MUST swap the local JSON state manager for an asynchronous database client (like MongoDB with `motor` or Redis via `aioredis`) to prevent blocking the event loop.

---

## 2. Testing Hell (Lock Event-Loop Mismatch)

This was by far the worst bug to debug. When running `pytest`, we kept getting:
`"error": "<asyncio.locks.Event object at ...> is bound to a different event loop"`

### Why?
Our Retriever Agent instance is defined as a global variable in `main.py`. This means it gets instantiated at import time in the main test runner.
But `pytest-asyncio` creates a brand new event loop for every single test case!
So when test A ran, the locks and events inside `ManualBatcher` got bound to test A's loop. When test B ran in a different loop, it tried to use those same locks, causing a loop mismatch error. The flusher task crashed, and the test hung forever waiting for futures that were never resolved.

### The Fix
We added a check at the top of `ManualBatcher.add()` and `TokenBucketRateLimiter.acquire()`:
```python
current_loop = asyncio.get_running_loop()
if not hasattr(self, "_loop") or self._loop != current_loop:
    self._loop = current_loop
    self.lock = asyncio.Lock()
    self.flush_event = asyncio.Event()
    self.background_task = None
```
If we detect a loop change (which happens between tests or worker thread shifts), we rebuild the locks and reset the background task references. This completely resolved the hangs, and the test suite now runs in **less than 5 seconds**.

---

## 3. Honest Architectural Trade-Offs

### Trade-Off 1: Bounded Queue Backpressure vs. Simple Queuing
- **We chose**: Bounding the queue size to 100 items.
- **Why**: Under heavy load, if a client is reading slowly, an unbounded queue will consume memory until the server dies of an OOM. Bounding it blocks the publisher when the queue is full.
- **The Cost**: If the client is slow, the agent executor will halt at `await queue.put()` and wait, which delays the entire DAG execution for that request. It's a classic tradeoff between memory stability and step throughput.

### Trade-Off 2: In-Memory Queue vs. Redis/RabbitMQ
- **We chose**: In-memory `asyncio.Queue`.
- **Why**: It is self-contained. A developer can just clone the repo and run the demo instantly without setting up Redis or Docker.
- **The Cost**: We cannot scale horizontally. If Uvicorn crashes, all running states are lost, and we can't resume executions across restarts.
- **Transition Point**: Once we pass 100 concurrent active users, we must migrate to Redis Streams.

---

## 4. Stuff I'd Fix If I Had Another Week

1. **Clean up metadata parameters**: Right now we inject `attempt_count` and `max_retries` into the agent's input dictionary. It works, but it's a bit messy. I should have used a separate `AgentContext` object.
2. **Prometheus Metrics**: Right now the metrics tracker is custom and in-memory. A real system would expose a `/metrics` scrape endpoint for Prometheus.
3. **Secrets Management**: I hardcoded some mock API targets in retriever.py. They should be in a `.env` file loaded via Pydantic Settings.
