import pytest
import time
import os
import json
from src.failure.circuit_breaker import CircuitBreaker, CircuitBreakerOpenException
from src.failure.handler import FailureHandler, DeadLetterQueue, TransientError, UnrecoverableError
from src.models import Step, RetryPolicy

def test_circuit_breaker_flow() -> None:
    """Verifies CircuitBreaker trips to OPEN on 3 failures, and enters HALF-OPEN after timeout."""
    # Cooldown 0.1s
    cb = CircuitBreaker(failure_threshold=3, recovery_timeout=0.1)
    
    # 1. Start in CLOSED state
    assert cb.get_state("retriever") == "CLOSED"
    cb.check("retriever") # Should not raise
    
    # 2. Record 2 failures (stays CLOSED)
    cb.record_failure("retriever")
    cb.record_failure("retriever")
    assert cb.get_state("retriever") == "CLOSED"
    
    # 3. Record 3rd failure (trips to OPEN)
    cb.record_failure("retriever")
    assert cb.get_state("retriever") == "OPEN"
    
    with pytest.raises(CircuitBreakerOpenException):
        cb.check("retriever")
        
    # 4. Wait for cooldown recovery_timeout (0.1s)
    time.sleep(0.15)
    
    # State should update to HALF-OPEN
    assert cb.get_state("retriever") == "HALF-OPEN"
    cb.check("retriever") # Should not raise in HALF-OPEN state (it's testing)
    
    # 5. Success reset state to CLOSED
    cb.record_success("retriever")
    assert cb.get_state("retriever") == "CLOSED"

def test_failure_handler_strategies() -> None:
    dlq_file = "/Users/adib/Desktop/Agentic Ai system/multi-agent-orchestrator/state/test_dlq.jsonl"
    if os.path.exists(dlq_file):
        os.remove(dlq_file)
        
    dlq = DeadLetterQueue(dlq_path=dlq_file)
    handler = FailureHandler(dlq=dlq)
    
    step = Step(
        step_id="step1",
        agent_type="retriever",
        inputs={"queries": ["AAPL", "INVALID_TICKET"]},
        retry_policy=RetryPolicy(max_retries=2)
    )
    
    # 1. First failure -> RETRY
    strategy, _ = handler.determine_strategy(step, TransientError("404"), retries_taken=1)
    assert strategy == "retry"
    
    # 2. Retries exhausted (retries_taken=2, max_retries=2) -> DEGRADE (since retriever has multiple queries)
    strategy, _ = handler.determine_strategy(step, TransientError("404"), retries_taken=2)
    assert strategy == "degrade"
    
    # 3. Retries exhausted on non-degradable (single query retriever) -> FALLBACK
    single_step = Step(
        step_id="step2",
        agent_type="retriever",
        inputs={"query": "AAPL"},
        retry_policy=RetryPolicy(max_retries=1)
    )
    strategy, fallback_agent = handler.determine_strategy(single_step, TransientError("404"), retries_taken=1)
    assert strategy == "fallback"
    assert fallback_agent == "fallback_retriever"
    
    # 4. UnrecoverableError -> ESCALATE immediately
    strategy, _ = handler.determine_strategy(single_step, UnrecoverableError("Syntax error"), retries_taken=0)
    assert strategy == "escalate"
    
    # Verify DLQ write
    assert os.path.exists(dlq_file)
    with open(dlq_file, "r") as f:
        dlq_entries = [json.loads(line) for line in f]
    assert len(dlq_entries) == 1
    assert dlq_entries[0]["step_id"] == "step2"
    assert "Syntax error" in dlq_entries[0]["error"]
    
    # Clean up
    if os.path.exists(dlq_file):
        os.remove(dlq_file)
