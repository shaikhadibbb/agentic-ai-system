import asyncio
from typing import List, Any, Callable, Tuple, Dict, Optional
from src.utils.logger import get_logger

logger = get_logger("manual_batcher")

class QueueFullException(Exception):
    """Exception raised when the batcher queue exceeds its maximum capacity."""
    pass

class ManualBatcher:
    """
    ManualBatcher pools database/API queries and runs them in bulk to avoid hammering
    downstream services. We wrote this ourselves to handle timeouts and backpressure
    properly without adding heavy external dependencies.
    """
    
    def __init__(self, batch_size: int, max_wait_ms: float, processor: Callable[[List[Any]], Any], max_queue_size: int = 100) -> None:
        """
        Args:
            batch_size: The number of items to trigger an immediate flush.
            max_wait_ms: The maximum wait time in milliseconds before flushing.
            processor: An async function that processes a List[Any] and returns a List[Any] of results.
            max_queue_size: The maximum size of the buffer before rejecting new additions.
        """
        self.batch_size = batch_size
        self.max_wait_ms = max_wait_ms / 1000.0  # Convert to seconds
        self.processor = processor
        self.max_queue_size = max_queue_size
        self.buffer: List[Tuple[Any, asyncio.Future[Any]]] = []
        self.lock = asyncio.Lock()
        self.flush_event = asyncio.Event()
        self.background_task: Optional[asyncio.Task[None]] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    async def add(self, item: Any) -> Any:
        """
        Adds a query/task to the buffer. Returns a future that will resolve
        when the batch is flushed and executed.
        """
        current_loop = asyncio.get_running_loop()
        if not hasattr(self, "_loop") or self._loop != current_loop:
            self._loop = current_loop
            self.lock = asyncio.Lock()
            self.flush_event = asyncio.Event()
            self.background_task = None
            
        fut = current_loop.create_future()
        
        async with self.lock:
            # Prevent buffer overflow / backpressure limits
            if len(self.buffer) >= self.max_queue_size:
                raise QueueFullException(f"ManualBatcher queue is full: {len(self.buffer)} items, max is {self.max_queue_size}")
                
            # Spawn flusher inside the lock to prevent race conditions.
            # If the background flusher task finished, restart it.
            if self.background_task is None or self.background_task.done():
                self.background_task = asyncio.create_task(self._background_flusher())
                
            self.buffer.append((item, fut))
            logger.info("Item added to batch buffer", buffer_size=len(self.buffer), batch_size=self.batch_size)
            if len(self.buffer) >= self.batch_size:
                logger.info("Batch size reached. Triggering flush event.")
                self.flush_event.set()
                
        return await fut

    async def flush(self) -> None:
        """
        Drains the buffer and runs the batch processor. Resolves all the waiting
        futures in one go, mapping results back to their callers.
        """
        async with self.lock:
            if not self.buffer:
                return
            current_batch = self.buffer
            self.buffer = []
            self.flush_event.clear()
            
        items = [item for item, _ in current_batch]
        futures = [fut for _, fut in current_batch]
        
        logger.info("Flushing batch", size=len(items))
        
        try:
            # Process everything in parallel. The processor handles individual errors and
            # returns a list of results (or exceptions) mapped index-for-index.
            results = await self.processor(items)
            
            # Resolve each future with its corresponding result
            for fut, res in zip(futures, results):
                if not fut.done():
                    if isinstance(res, Exception):
                        fut.set_exception(res)
                    else:
                        fut.set_result(res)
        except Exception as e:
            logger.error("Batch processing failed critically", error=str(e))
            # Fail all futures in this batch so they aren't left hanging forever
            for _, fut in current_batch:
                if not fut.done():
                    fut.set_exception(e)

    async def _background_flusher(self) -> None:
        """Ticking clock that wakes up to flush the batch if it sits around for too long."""
        try:
            while True:
                try:
                    # Wait for either the flush event or the timeout
                    await asyncio.wait_for(self.flush_event.wait(), timeout=self.max_wait_ms)
                except asyncio.TimeoutError:
                    # Timeout reached, flush if any items exist
                    pass
                
                # Check and flush if buffer is not empty
                if len(self.buffer) > 0:
                    logger.info("Timeout elapsed. Flushing partial batch.")
                    await self.flush()
        except asyncio.CancelledError:
            logger.info("Background flusher cancelled")
        except Exception as e:
            logger.error("Background flusher error", error=str(e))

    async def shutdown(self) -> None:
        """Cleans up the flusher task and does a final flush so we don't lose buffered data."""
        if self.background_task is not None:
            self.background_task.cancel()
            try:
                loop = asyncio.get_running_loop()
                if self.background_task.get_loop() == loop:
                    await self.background_task
            except (asyncio.CancelledError, RuntimeError):
                pass
        await self.flush()
class BatchProcessorResult:
    """Represents a structured result for an individual item in a batch."""
    def __init__(self, status: str, data: Any = None, error: Optional[str] = None):
        self.status = status  # "success" or "error"
        self.data = data
        self.error = error
        
    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status,
            "data": self.data,
            "error": self.error
        }
