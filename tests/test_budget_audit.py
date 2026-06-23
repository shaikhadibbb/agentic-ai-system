import pytest
import asyncio
from typing import Dict, Any
from src.utils.budget_audit import BudgetTracker, BudgetExhausted, generate_hash
from src.executor import DAGExecutor
from src.models import ExecutionPlan, Step
from src.agents.base import BaseAgent
from src.failure.handler import FailureHandler
from src.failure.circuit_breaker import CircuitBreaker
from src.failure.state_manager import StateManager

class DummyAgent(BaseAgent):
    async def run(self, inputs: Dict[str, Any], queue: asyncio.Queue) -> Dict[str, Any]:
        return {"result": "success"}

@pytest.mark.asyncio
async def test_budget_tracker_enforcement() -> None:
    """Verifies that BudgetTracker enforces limits and raises BudgetExhausted."""
    tracker = BudgetTracker(max_cost_cents=15)
    
    # 1. Successful charges
    assert await tracker.charge(10, "First step") is True
    assert tracker.spent == 10
    
    # 2. Exceeds remaining budget
    with pytest.raises(BudgetExhausted):
        await tracker.charge(10, "Second step")

@pytest.mark.asyncio
async def test_executor_budget_enforcement() -> None:
    """Verifies that the DAGExecutor aborts execution when the budget is exhausted."""
    # Set max cost to 5 cents (running retriever costs 10, so it will exhaust immediately)
    tracker = BudgetTracker(max_cost_cents=5)
    
    step_a = Step(
        step_id="stepA",
        agent_type="retriever",
        inputs={"query": "test"},
        dependencies=[]
    )
    plan = ExecutionPlan(steps={"stepA": step_a})
    
    agents = {"retriever": DummyAgent()}
    state_manager = StateManager(storage_dir="/Users/adib/Desktop/Agentic Ai system/multi-agent-orchestrator/state/test_state")
    
    executor = DAGExecutor(
        agents=agents,
        failure_handler=FailureHandler(),
        circuit_breaker=CircuitBreaker(),
        state_manager=state_manager,
        budget_tracker=tracker
    )
    
    queue = asyncio.Queue()
    with pytest.raises(BudgetExhausted):
        await executor.execute(plan, queue, "test-budget-exec")
        
    state_manager.delete_state("test-budget-exec")

@pytest.mark.asyncio
async def test_audit_receipt_generation() -> None:
    """Verifies that the executor creates and chains audit receipts for completed steps."""
    tracker = BudgetTracker(max_cost_cents=100)
    
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
    
    agents = {
        "retriever": DummyAgent(),
        "analyzer": DummyAgent()
    }
    state_manager = StateManager(storage_dir="/Users/adib/Desktop/Agentic Ai system/multi-agent-orchestrator/state/test_state")
    
    executor = DAGExecutor(
        agents=agents,
        failure_handler=FailureHandler(),
        circuit_breaker=CircuitBreaker(),
        state_manager=state_manager,
        budget_tracker=tracker
    )
    
    queue = asyncio.Queue()
    await executor.execute(plan, queue, "test-audit-exec")
    
    # Check receipt chain
    assert "stepA" in executor.receipts
    assert "stepB" in executor.receipts
    
    receipt_a = executor.receipts["stepA"]
    receipt_b = executor.receipts["stepB"]
    
    assert receipt_a.agent_name == "retriever"
    assert receipt_b.agent_name == "analyzer"
    
    # Chained custody verification: Step B parent receipt ID should link to Step A's output hash
    assert receipt_b.parent_receipt_id == receipt_a.output_hash
    
    # Hash verification
    expected_output_hash = generate_hash({"result": "success"})
    assert receipt_a.output_hash == expected_output_hash
    
    state_manager.delete_state("test-audit-exec")
