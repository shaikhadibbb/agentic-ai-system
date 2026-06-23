import pytest
import asyncio
from typing import Dict, Any
from src.executor import DAGExecutor, resolve_inputs
from src.models import ExecutionPlan, Step, OutputChunk, RetryPolicy
from src.agents.base import BaseAgent
from src.failure.handler import FailureHandler
from src.failure.circuit_breaker import CircuitBreaker
from src.failure.state_manager import StateManager

class MockAgent(BaseAgent):
    def __init__(self, val: str) -> None:
        self.val = val
        
    async def run(self, inputs: Dict[str, Any], queue: asyncio.Queue) -> Dict[str, Any]:
        step_id = inputs.get("step_id", "mock")
        # Extract inputs
        dep_data = inputs.get("data", "")
        # Emit running update
        await queue.put(OutputChunk(step_id=step_id, content=f"Mock running {self.val}", status="running"))
        await asyncio.sleep(0.05)
        # Emit completed update
        await queue.put(OutputChunk(step_id=step_id, content=f"Mock completed {self.val}", status="completed"))
        return {"output_val": f"{self.val}_{dep_data}" if dep_data else self.val}

def test_resolve_inputs() -> None:
    step = Step(
        step_id="stepB",
        agent_type="analyzer",
        inputs={
            "data": "step_id:stepA",
            "regions": ["USA", "step_id:stepA", "EU"],
            "constant": 42
        }
    )
    completed_outputs = {"stepA": {"output_val": "resultA"}}
    resolved = resolve_inputs(step, completed_outputs)
    
    assert resolved["data"] == {"output_val": "resultA"}
    assert resolved["regions"] == ["USA", {"output_val": "resultA"}, "EU"]
    assert resolved["constant"] == 42

@pytest.mark.asyncio
async def test_dag_executor_linear_flow() -> None:
    # Build 2-step plan: stepA -> stepB
    step_a = Step(
        step_id="stepA",
        agent_type="retriever",
        inputs={"query": "test"},
        dependencies=[]
    )
    step_b = Step(
        step_id="stepB",
        agent_type="analyzer",
        inputs={"data": "step_id:stepA"},
        dependencies=["stepA"]
    )
    plan = ExecutionPlan(steps={"stepA": step_a, "stepB": step_b})
    
    # Initialize components
    agents = {
        "retriever": MockAgent("A"),
        "analyzer": MockAgent("B")
    }
    
    # Storage dir inside workspace
    storage_dir = "/Users/adib/Desktop/Agentic Ai system/multi-agent-orchestrator/state/test_state"
    state_manager = StateManager(storage_dir=storage_dir)
    failure_handler = FailureHandler()
    circuit_breaker = CircuitBreaker()
    
    executor = DAGExecutor(
        agents=agents,
        failure_handler=failure_handler,
        circuit_breaker=circuit_breaker,
        state_manager=state_manager
    )
    
    queue = asyncio.Queue()
    execution_id = "test-exec-1"
    
    outputs = await executor.execute(plan, queue, execution_id)
    
    assert "stepA" in outputs
    assert "stepB" in outputs
    assert outputs["stepA"] == {"output_val": "A"}
    assert outputs["stepB"] == {"output_val": "B_{'output_val': 'A'}"}
    
    # Assert updates were streamed to the queue
    chunks = []
    while not queue.empty():
        chunks.append(queue.get_nowait())
        
    # We should have running/completed chunks, plus metrics
    assert len(chunks) >= 5
    assert any(c.step_id == "stepA" and c.status == "completed" for c in chunks)
    assert any(c.step_id == "stepB" and c.status == "completed" for c in chunks)
    assert chunks[-1].step_id == "system_metrics"
    
    # Clean up state file
    state_manager.delete_state(execution_id)


class FailingAgent(BaseAgent):
    def __init__(self, fail_count: int, success_val: str) -> None:
        self.fail_count = fail_count
        self.success_val = success_val
        self.calls = 0

    async def run(self, inputs: Dict[str, Any], queue: asyncio.Queue) -> Dict[str, Any]:
        self.calls += 1
        if self.calls <= self.fail_count:
            from src.failure.handler import TransientError
            raise TransientError(f"Simulated transient error call {self.calls}")
        return {"output_val": self.success_val}

@pytest.mark.asyncio
async def test_dag_executor_retry_flow() -> None:
    # 2 retries allowed
    step_a = Step(
        step_id="stepA",
        agent_type="retriever",
        inputs={"query": "test"},
        retry_policy=RetryPolicy(max_retries=2, initial_delay=0.01),
        dependencies=[]
    )
    plan = ExecutionPlan(steps={"stepA": step_a})
    
    # Failing agent that fails 1 time and succeeds on 2nd call (1 retry needed)
    agents = {"retriever": FailingAgent(fail_count=1, success_val="success_after_fail")}
    
    state_manager = StateManager(storage_dir="/Users/adib/Desktop/Agentic Ai system/multi-agent-orchestrator/state/test_state")
    executor = DAGExecutor(
        agents=agents,
        failure_handler=FailureHandler(),
        circuit_breaker=CircuitBreaker(),
        state_manager=state_manager
    )
    
    queue = asyncio.Queue()
    execution_id = "test-exec-retry"
    outputs = await executor.execute(plan, queue, execution_id)
    
    assert outputs["stepA"] == {"output_val": "success_after_fail"}
    
    # Check that retry logs were pushed to queue
    chunks = []
    while not queue.empty():
        chunks.append(queue.get_nowait())
    assert any(c.status == "retry" for c in chunks)
    
    state_manager.delete_state(execution_id)

def test_state_manager_details() -> None:
    from src.models import ExecutionState
    storage_dir = "/Users/adib/Desktop/Agentic Ai system/multi-agent-orchestrator/state/test_state"
    state_manager = StateManager(storage_dir=storage_dir)
    
    # 1. Load non-existent state
    non_existent = state_manager.load_state("non_existent_id")
    assert non_existent is None
    
    # 2. Save and Load
    step_a = Step(step_id="stepA", agent_type="retriever")
    plan = ExecutionPlan(steps={"stepA": step_a})
    state = ExecutionState(execution_id="test-sm", plan=plan)
    
    state_manager.save_state(state)
    loaded = state_manager.load_state("test-sm")
    assert loaded is not None
    assert loaded.execution_id == "test-sm"
    
    # 3. Delete
    state_manager.delete_state("test-sm")
    assert state_manager.load_state("test-sm") is None

