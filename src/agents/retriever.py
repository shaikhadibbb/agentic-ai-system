import asyncio
import time
from typing import List, Dict, Any, Optional
from collections import OrderedDict
from src.agents.base import BaseAgent
from src.models import RetrievalResult, OutputChunk
from src.utils.batcher import ManualBatcher
from src.utils.logger import get_logger
from src.failure.handler import TransientError

logger = get_logger("retriever_agent")

class TokenBucketRateLimiter:
    """
    stole this rate limiter design from stackoverflow then broke it, then fixed it.
    manually refilling tokens on request to keep our api limits clean without
    bloating the project with external libraries.
    """
    def __init__(self, capacity: int, refill_rate_per_sec: float) -> None:
        self.capacity = capacity
        self.refill_rate = refill_rate_per_sec
        self.tokens = float(capacity)
        self.last_refill = time.time()
        self.lock = asyncio.Lock()
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    async def acquire(self, tokens: float = 1.0) -> None:
        """Blocks until we get enough tokens. loops with asyncio.sleep."""
        current_loop = asyncio.get_running_loop()
        if not hasattr(self, "_loop") or self._loop != current_loop:
            self._loop = current_loop
            self.lock = asyncio.Lock()
            
        while True:
            async with self.lock:
                now = time.time()
                elapsed = now - self.last_refill
                # Refill bucket
                self.tokens = min(float(self.capacity), self.tokens + elapsed * self.refill_rate)
                self.last_refill = now
                
                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return
            await asyncio.sleep(0.05)

class LRUCache:
    """A simple dictionary-based LRU cache from first principles."""
    def __init__(self, capacity: int) -> None:
        self.capacity = capacity
        self.cache: OrderedDict[str, Any] = OrderedDict()

    def get(self, key: str) -> Optional[Any]:
        if key not in self.cache:
            return None
        self.cache.move_to_end(key)
        return self.cache[key]

    def put(self, key: str, value: Any) -> None:
        if key in self.cache:
            self.cache.move_to_end(key)
        self.cache[key] = value
        if len(self.cache) > self.capacity:
            self.cache.popitem(last=False)

class RetrieverAgent(BaseAgent):
    """
    Retriever Agent: Performs token-rate-limited, LRU cached, batched data retrieval 
    from mock external services (arXiv, Wikipedia, Bloomberg, etc.).
    """

    def __init__(self, batch_size: int = 4, max_wait_ms: float = 200.0) -> None:
        self.rate_limiter = TokenBucketRateLimiter(capacity=10, refill_rate_per_sec=5.0)
        self.cache = LRUCache(capacity=50)
        self.simulate_failure = False
        
        # Initialize manual batcher mapping to the batch processor
        self.batcher = ManualBatcher(
            batch_size=batch_size, 
            max_wait_ms=max_wait_ms, 
            processor=self._batch_retrieve
        )
        
        # Simulated mock database
        self.mock_db = {
            "aapl": {
                "content": "AAPL stock price is $175.50. Market trends show a steady 3% upward growth quarter-over-quarter.",
                "source": "Bloomberg Finance",
                "confidence": 0.98
            },
            "googl": {
                "content": "GOOGL stock price is $150.25. Alphabet reports strong advertising and cloud revenue.",
                "source": "Nasdaq Realtime",
                "confidence": 0.97
            },
            "msft": {
                "content": "MSFT stock price is $420.10. AI integrations into Office 365 and Azure drive robust growth.",
                "source": "Reuters",
                "confidence": 0.99
            },
            "ai breakthroughs in protein folding": {
                "content": "AlphaFold 3 predicts 3D structures of proteins, DNA, RNA, chemical modifications, and ligand complexes with unprecedented accuracy, accelerating drug discovery.",
                "source": "arXiv:2405.08805",
                "confidence": 0.95
            },
            "climate policies of eu from 2023-2024": {
                "content": "EU implemented the Carbon Border Adjustment Mechanism (CBAM) in 2023. Critics flag contradictions, such as Germany increasing short-term coal usage to offset nuclear phase-outs.",
                "source": "European Environment Agency",
                "confidence": 0.92
            },
            "climate policies of usa from 2023-2024": {
                "content": "USA Biden administration allocates $369B under the Inflation Reduction Act. However, oil production reached record highs in 2023, exposing policy contradictions.",
                "source": "EPA Climate Registry",
                "confidence": 0.91
            },
            "climate policies of china from 2023-2024": {
                "content": "China installed more solar capacity in 2023 than the US cumulative total. However, China continues approving new coal power plants to secure grid reliability, raising contradictions.",
                "source": "Climate Action Tracker",
                "confidence": 0.89
            }
        }

    async def _batch_retrieve(self, queries: List[str]) -> List[Any]:
        """Processor method executed by ManualBatcher in parallel."""
        logger.info("Executing batched retrieval processor", queries=queries)
        results = []
        for q in queries:
            # 1. Manual rate limit acquisition
            await self.rate_limiter.acquire(1.0)
            
            # 2. Check cache
            normalized_query = q.strip().lower()
            cached_val = self.cache.get(normalized_query)
            if cached_val is not None:
                logger.info("Cache hit", query=q)
                results.append(cached_val)
                continue
                
            # 3. Simulate processing delay
            await asyncio.sleep(0.05)
            
            # 4. Handle invalid ticket mock transient error
            if "invalid_ticket" in normalized_query:
                logger.warn("Simulating transient API failure for query", query=q)
                results.append(TransientError(f"Mock API 404: Ticket '{q}' not found"))
                continue
                
            # 5. Fetch from mock database
            matched_data: Optional[Dict[str, Any]] = None
            # simple substring matching
            for key, val in self.mock_db.items():
                if key in normalized_query or normalized_query in key:
                    matched_data = val
                    break
                    
            if matched_data:
                ret_result = RetrievalResult(
                    query=q,
                    source=matched_data["source"],
                    content=matched_data["content"],
                    confidence=matched_data["confidence"]
                )
                self.cache.put(normalized_query, ret_result)
                results.append(ret_result)
            else:
                # General query fallback
                ret_result = RetrievalResult(
                    query=q,
                    source="Wikipedia Search",
                    content=f"No specialized mock data for '{q}'. Retrieved general knowledge about {q}.",
                    confidence=0.70
                )
                self.cache.put(normalized_query, ret_result)
                results.append(ret_result)
                
        return results

    async def run(self, inputs: Dict[str, Any], queue: asyncio.Queue[Any]) -> Dict[str, Any]:
        """
        Executes Retriever Agent.
        """
        if getattr(self, "simulate_failure", False):
            raise TransientError("Simulated API connection failure: connection timeout to premium source.")
            
        queries: List[str] = []
        if "query" in inputs:
            queries = [inputs["query"]]
        elif "queries" in inputs:
            queries = inputs["queries"]
            
        attempt_count = inputs.get("attempt_count", 1)
        max_retries = inputs.get("max_retries", 1)
        
        step_id = inputs.get("step_id", "retriever")
        
        await queue.put(OutputChunk(
            step_id=step_id, 
            content=f"Submitting {len(queries)} retrieval request(s) to manual batcher...", 
            status="running"
        ))
        
        # Add all queries to batcher
        futures = []
        for q in queries:
            fut = self.batcher.add(q)
            futures.append(fut)
            
        # Run in parallel, wait for batcher
        raw_results = await asyncio.gather(*futures, return_exceptions=True)
        
        successes: List[RetrievalResult] = []
        failures: List[Dict[str, Any]] = []
        
        for q, res in zip(queries, raw_results):
            if isinstance(res, Exception):
                failures.append({"query": q, "error": str(res), "exception": res})
            elif isinstance(res, RetrievalResult):
                successes.append(res)
                
        if failures:
            # If there are failures, check if we are allowed to degrade (e.g. final attempt or attempt_count > max_retries)
            # Or if this is a single query failure, we must propagate the failure to trigger standard retry.
            is_final_attempt = attempt_count > max_retries
            
            # Scenario B requirement: returns successes + warning
            if successes and (is_final_attempt or len(queries) > 1):
                warning_msg = f"Partial failure handled: {len(failures)} item(s) failed. Continuing with degraded data."
                await queue.put(OutputChunk(
                    step_id=step_id,
                    content=warning_msg,
                    status="warning"
                ))
                return {
                    "status": "degraded",
                    "results": [s.model_dump() for s in successes],
                    "errors": [{"query": f["query"], "error": f["error"]} for f in failures],
                    "warning": warning_msg
                }
            else:
                # Propagate the first error to the executor to trigger retry/escalate
                err = failures[0]["exception"]
                logger.error("Retrieval failed, propagating exception", step_id=step_id, error=str(err))
                raise err
                
        await queue.put(OutputChunk(
            step_id=step_id,
            content=f"Successfully retrieved all {len(successes)} item(s).",
            status="completed"
        ))
        
        return {
            "status": "success",
            "results": [s.model_dump() for s in successes]
        }
