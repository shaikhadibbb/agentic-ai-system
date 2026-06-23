import pytest
import asyncio
from typing import Dict, Any
from src.executor import DAGExecutor
from src.models import ExecutionPlan, Step, RetryPolicy
from src.agents.base import BaseAgent
from src.failure.handler import FailureHandler, TransientError
from src.failure.circuit_breaker import CircuitBreaker, CircuitBreakerOpenException
from src.failure.state_manager import StateManager

class HangingAgent(BaseAgent):
    async def run(self, inputs: Dict[str, Any], queue: asyncio.Queue[Any]) -> Dict[str, Any]:
        # Hang indefinitely
        await asyncio.sleep(10.0)
        return {"result": "done"}

class FailingAgent(BaseAgent):
    async def run(self, inputs: Dict[str, Any], queue: asyncio.Queue[Any]) -> Dict[str, Any]:
        raise ValueError("Simulated failure")

@pytest.mark.asyncio
async def test_step_timeout_enforcement() -> None:
    """Verifies that an agent run is cancelled if it exceeds step.timeout."""
    # Set max_retries to 0 so it falls back immediately, and register both primary and fallback
    step_a = Step(
        step_id="stepA",
        agent_type="retriever",
        inputs={"query": "test"},
        timeout=0.05,  # Very short timeout to trigger quickly
        retry_policy=RetryPolicy(max_retries=0),
        dependencies=[]
    )
    plan = ExecutionPlan(steps={"stepA": step_a})
    
    # Both primary and fallback are hanging agents
    agents = {
        "retriever": HangingAgent(),
        "fallback_retriever": HangingAgent()
    }
    
    state_manager = StateManager(storage_dir="/Users/adib/Desktop/Agentic Ai system/multi-agent-orchestrator/state/test_state")
    
    executor = DAGExecutor(
        agents=agents,
        failure_handler=FailureHandler(),
        circuit_breaker=CircuitBreaker(),
        state_manager=state_manager
    )
    
    queue = asyncio.Queue()
    execution_id = "test-timeout-exec"
    
    # Executing the plan should raise TransientError due to step timeout in fallback
    with pytest.raises(TransientError) as exc_info:
        await executor.execute(plan, queue, execution_id)
        
    assert "Step timed out after 0.05s" in str(exc_info.value)
    state_manager.delete_state(execution_id)

@pytest.mark.asyncio
async def test_fallback_infinite_loop_prevention() -> None:
    """Verifies that fallback routing escalates instead of infinite looping if the fallback agent also fails."""
    step_a = Step(
        step_id="stepA",
        agent_type="retriever",
        inputs={"query": "test"},
        dependencies=[]
    )
    plan = ExecutionPlan(steps={"stepA": step_a})
    
    # Both primary and fallback agents will fail
    agents = {
        "retriever": FailingAgent(),
        "fallback_retriever": FailingAgent()
    }
    
    # Trip both circuit breakers
    cb = CircuitBreaker(failure_threshold=1, recovery_timeout=10.0)
    cb.record_failure("retriever")
    cb.record_failure("fallback_retriever")
    
    state_manager = StateManager(storage_dir="/Users/adib/Desktop/Agentic Ai system/multi-agent-orchestrator/state/test_state")
    executor = DAGExecutor(
        agents=agents,
        failure_handler=FailureHandler(),
        circuit_breaker=cb,
        state_manager=state_manager
    )
    
    queue = asyncio.Queue()
    execution_id = "test-loop-prevention"
    
    # Execution should raise CircuitBreakerOpenException and terminate (escalate) cleanly
    with pytest.raises(CircuitBreakerOpenException):
        await executor.execute(plan, queue, execution_id)
        
    state_manager.delete_state(execution_id)
