import pytest
import asyncio
from typing import Dict, Any
from src.executor import DAGExecutor
from src.models import ExecutionPlan, Step
from src.agents.base import BaseAgent
from src.failure.handler import FailureHandler
from src.failure.circuit_breaker import CircuitBreaker
from src.failure.state_manager import StateManager
from src.utils.logger import get_logger

logger = get_logger("test_circuit_fallback")

class DummyAgent(BaseAgent):
    def __init__(self, val: str) -> None:
        self.val = val

    async def run(self, inputs: Dict[str, Any], queue: asyncio.Queue[Any]) -> Dict[str, Any]:
        return {"result": self.val}

@pytest.mark.asyncio
async def test_circuit_breaker_trips_and_uses_fallback() -> None:
    """
    Verifies that when the primary agent's circuit breaker is tripped,
    the DAGExecutor seamlessly routes the step to the fallback agent.
    """
    # 1. Setup agents: primary and fallback
    agents = {
        "retriever": DummyAgent("primary_retriever_result"),
        "fallback_retriever": DummyAgent("fallback_retriever_result")
    }

    # 2. Trip the circuit breaker for primary "retriever"
    cb = CircuitBreaker(failure_threshold=3, recovery_timeout=10.0)
    cb.record_failure("retriever")
    cb.record_failure("retriever")
    cb.record_failure("retriever")
    assert cb.get_state("retriever") == "OPEN"

    # 3. Create execution plan with one step using "retriever"
    step_a = Step(
        step_id="stepA",
        agent_type="retriever",
        inputs={"query": "test"},
        dependencies=[]
    )
    plan = ExecutionPlan(steps={"stepA": step_a})

    state_manager = StateManager(storage_dir="/Users/adib/Desktop/Agentic Ai system/multi-agent-orchestrator/state/test_state")
    executor = DAGExecutor(
        agents=agents,
        failure_handler=FailureHandler(),
        circuit_breaker=cb,
        state_manager=state_manager
    )

    queue: asyncio.Queue[Any] = asyncio.Queue()
    execution_id = "test-cb-fallback-exec"

    # 4. Execute the plan. It should fall back to "fallback_retriever" and complete
    outputs = await executor.execute(plan, queue, execution_id)

    # Clean up state file
    state_manager.delete_state(execution_id)

    # 5. Assert the output result is from the fallback agent
    assert outputs["stepA"] == {"result": "fallback_retriever_result"}

    # Assert warnings/status chunks were streamed
    chunks = []
    while not queue.empty():
        chunks.append(queue.get_nowait())

    # Verify that we got a warning chunk about the circuit breaker and fallback swap
    assert any(c.status == "warning" and "CB Open for 'retriever'" in str(c.content) for c in chunks)
