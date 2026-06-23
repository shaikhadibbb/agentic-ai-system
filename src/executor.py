import asyncio
from typing import Dict, Any, List, Set, Optional, Tuple
from src.models import ExecutionPlan, Step, OutputChunk, ExecutionState
from src.agents.base import BaseAgent
from src.failure.handler import FailureHandler, UnrecoverableError, TransientError
from src.failure.circuit_breaker import CircuitBreaker, CircuitBreakerOpenException
from src.failure.state_manager import StateManager
from src.utils.logger import get_logger
from src.utils.metrics import MetricsTracker
from src.utils.budget_audit import BudgetTracker, AuditReceipt, generate_hash

logger = get_logger("dag_executor")

def resolve_inputs(step: Step, completed_outputs: Dict[str, Any]) -> Dict[str, Any]:
    """
    Stitches step inputs together by swapping placeholders like 'step_id:whatever'
    with real data from parent steps that already finished. its kinda hacky but works.
    """
    resolved = {}
    for key, val in step.inputs.items():
        if isinstance(val, str) and val.startswith("step_id:"):
            dep_step_id = val.split(":", 1)[1]
            resolved[key] = completed_outputs.get(dep_step_id)
        elif isinstance(val, list):
            resolved_list = []
            for item in val:
                if isinstance(item, str) and item.startswith("step_id:"):
                    dep_step_id = item.split(":", 1)[1]
                    resolved_list.append(completed_outputs.get(dep_step_id))
                else:
                    resolved_list.append(item)
            resolved[key] = resolved_list
        else:
            resolved[key] = val
    return resolved

class DAGExecutor:
    """
    DAGExecutor executes an ExecutionPlan containing a DAG of steps.
    We use asyncio primitives directly instead of Celery/RQ to maintain
    tight control over execution flow. Much lighter this way.
    """

    def __init__(
        self,
        agents: Dict[str, BaseAgent],
        failure_handler: FailureHandler,
        circuit_breaker: CircuitBreaker,
        state_manager: StateManager,
        budget_tracker: Optional[BudgetTracker] = None
    ) -> None:
        self.agents = agents
        self.failure_handler = failure_handler
        self.circuit_breaker = circuit_breaker
        self.state_manager = state_manager
        self.budget_tracker = budget_tracker or BudgetTracker()
        self.receipts: Dict[str, AuditReceipt] = {}

    async def execute(self, plan: ExecutionPlan, queue: asyncio.Queue[Any], execution_id: str, budget_tracker: Optional[BudgetTracker] = None) -> Dict[str, Any]:
        """
        Executes the plan topologically, running independent steps in parallel.
        Pushes OutputChunk updates to the streaming queue.
        """
        metrics = MetricsTracker()
        steps = plan.steps
        
        # Use request-scoped budget tracker if provided to prevent crosstalk/exhaustion between requests
        active_budget = budget_tracker or self.budget_tracker
        
        logger.info("Starting DAG execution", execution_id=execution_id, total_steps=len(steps))
        
        # 1. State Preservation: Initialize and save execution state
        state = ExecutionState(execution_id=execution_id, plan=plan)
        for sid in steps:
            state.step_states[sid] = {
                "status": "pending",
                "input": None,
                "output": None,
                "retries_taken": 0,
                "error": None
            }
        self.state_manager.save_state(state)
        
        # 2. Build In-degree and Adjacency List for topological scheduling.
        # Kahn's algorithm: I wrote this topological sort manually because importing networkx
        # felt like cheating, and also I didn't want to deal with its weird API.
        # Track in-degrees (parent count) and child lists so we can fire off independent nodes in parallel.
        # Spent 3 hours debugging this because I forgot to decrement in-degree on failure. Fixed now.
        in_degree = {step_id: len(step.dependencies) for step_id, step in steps.items()}
        adj_list: Dict[str, List[str]] = {step_id: [] for step_id in steps}
        for step_id, step in steps.items():
            for dep in step.dependencies:
                adj_list[dep].append(step_id)
                
        completed_outputs: Dict[str, Any] = {}
        completed_steps: Set[str] = set()
        running_tasks: Dict[str, asyncio.Task[None]] = {}
        
        state_lock = asyncio.Lock()
        finished_queue: asyncio.Queue[Tuple[str, str, Optional[Exception]]] = asyncio.Queue()
        
        # Helper task function to wrap execution of a single node in the DAG
        async def run_node(step_id: str) -> None:
            step = steps[step_id]
            inputs_resolved = resolve_inputs(step, completed_outputs)
            
            async with state_lock:
                state.step_states[step_id]["status"] = "running"
                state.step_states[step_id]["input"] = inputs_resolved
                self.state_manager.save_state(state)
                
            try:
                # Run the step with fault tolerance (retry/fallback/degrade)
                output = await self._execute_step_with_fault_tolerance(
                    step_id, step, plan, queue, execution_id, inputs_resolved, metrics, budget_tracker=active_budget
                )
                
                async with state_lock:
                    completed_outputs[step_id] = output
                    state.step_states[step_id]["status"] = "completed"
                    state.step_states[step_id]["output"] = output
                    self.state_manager.save_state(state)
                    
                await finished_queue.put((step_id, "success", None))
            except Exception as e:
                logger.error("Step execution failed", step_id=step_id, error=str(e))
                async with state_lock:
                    state.step_states[step_id]["status"] = "failed"
                    state.step_states[step_id]["error"] = str(e)
                    self.state_manager.save_state(state)
                await finished_queue.put((step_id, "failed", e))

        # 3. Schedule initial nodes (in-degree == 0) and process topological loop
        try:
            for step_id, deg in in_degree.items():
                if deg == 0:
                    running_tasks[step_id] = asyncio.create_task(run_node(step_id))
                    
            # 4. Process topological execution loop
            while len(completed_steps) < len(steps):
                if not running_tasks and len(completed_steps) < len(steps):
                    # Graph traversal stalled due to missing or blocked steps
                    raise RuntimeError("DAG execution halted unexpectedly: check for cycle or unhandled faults.")
                    
                # Wait for the next task to finish
                step_id, status, error = await finished_queue.get()
                
                if step_id in running_tasks:
                    del running_tasks[step_id]
                    
                if status == "failed":
                    # Escalate: cancel all other active tasks immediately
                    logger.warn("Escalating failure. Cancelling remaining tasks.", failed_step=step_id)
                    for task in list(running_tasks.values()):
                        task.cancel()
                    if running_tasks:
                        await asyncio.gather(*running_tasks.values(), return_exceptions=True)
                    if error is not None:
                        raise error
                    else:
                        raise RuntimeError("Step failed without an exception object.")
                    
                completed_steps.add(step_id)
                
                # Release dependents
                for dep in adj_list[step_id]:
                    in_degree[dep] -= 1
                    if in_degree[dep] == 0:
                        running_tasks[dep] = asyncio.create_task(run_node(dep))
                        
            # 5. Finalize state
            async with state_lock:
                state.is_completed = True
                self.state_manager.save_state(state)
        except asyncio.CancelledError:
            logger.warn("DAG execution cancelled", execution_id=execution_id)
            # Cancel all currently active tasks
            for task in list(running_tasks.values()):
                task.cancel()
            if running_tasks:
                await asyncio.gather(*running_tasks.values(), return_exceptions=True)
            # Transition running steps to failed/cancelled state on disk
            async with state_lock:
                for sid, sstate in state.step_states.items():
                    if sstate["status"] == "running":
                        sstate["status"] = "failed"
                        sstate["error"] = "Execution task cancelled."
                self.state_manager.save_state(state)
            raise
            
        # Append final timing metrics to the queue for the stream consumer
        summary_metrics = metrics.get_summary()
        await queue.put(OutputChunk(
            step_id="system_metrics",
            content=summary_metrics,
            status="completed"
        ))
        
        return completed_outputs

    async def _execute_step_with_fault_tolerance(
        self,
        step_id: str,
        step: Step,
        plan: ExecutionPlan,
        queue: asyncio.Queue[Any],
        execution_id: str,
        resolved_inputs: Dict[str, Any],
        metrics: MetricsTracker,
        agent_override: Optional[str] = None,
        budget_tracker: Optional[BudgetTracker] = None
    ) -> Dict[str, Any]:
        """
        Runs a single node with all the fallback & retry safety net logic.
        This method is a beast but it handles retries, swap to fallback agents, and circuit breakers.
        """
        agent_type = agent_override or step.agent_type
        attempt = 1
        
        # Use request-scoped budget tracker if provided
        active_budget = budget_tracker or self.budget_tracker
        
        while True:
            # 1. Circuit Breaker Check
            try:
                self.circuit_breaker.check(agent_type)
            except CircuitBreakerOpenException as cbe:
                # Handle CB open by querying strategy immediately
                strategy, fallback_agent = self.failure_handler.determine_strategy(step, cbe, attempt - 1, current_agent=agent_type)
                if strategy == "fallback" and fallback_agent:
                    await queue.put(OutputChunk(
                        step_id=step_id,
                        content=f"⚠️ CB Open for '{agent_type}'. Swapping to fallback agent '{fallback_agent}'...",
                        status="warning"
                    ))
                    agent_type = fallback_agent
                    attempt = 1
                    continue
                else:
                    await queue.put(OutputChunk(
                        step_id=step_id,
                        content=f"❌ Circuit is OPEN for '{agent_type}'. Execution halted.",
                        status="failed"
                    ))
                    raise cbe
                    
            # Retrieve agent instance
            agent = self.agents.get(agent_type)
            if not agent:
                raise UnrecoverableError(f"Agent instance for type '{agent_type}' is not registered.")
                
            # Inject execution metadata to help agent logic coordinate
            inputs_with_meta = {
                **resolved_inputs,
                "step_id": step_id,
                "attempt_count": attempt,
                "max_retries": step.retry_policy.max_retries
            }
            
            # Simulated charges so we don't blow up our mock API bill.
            cost_map = {
                "retriever": 10,
                "fallback_retriever": 5,
                "analyzer": 5,
                "fallback_analyzer": 3,
                "writer": 15
            }
            cost = cost_map.get(agent_type, 2)
            await active_budget.charge(cost, f"Execute step {step_id}")

            metrics.start_step(step_id)
            await queue.put(OutputChunk(
                step_id=step_id,
                content=f"Running step (Attempt {attempt})...",
                status="running"
            ))
            
            try:
                output = await asyncio.wait_for(agent.run(inputs_with_meta, queue), timeout=step.timeout)
                
                # Cryptographic audit receipt chain. Tbh this is probably overkill
                # but it makes the output look super secure.
                in_hash = generate_hash(resolved_inputs)
                out_hash = generate_hash(output)
                parent_id = None
                for dep in step.dependencies:
                    if dep in self.receipts:
                        parent_id = self.receipts[dep].output_hash
                        break
                receipt = AuditReceipt(
                    step_id=step_id,
                    agent_name=agent_type,
                    input_hash=in_hash,
                    output_hash=out_hash,
                    parent_receipt_id=parent_id
                )
                self.receipts[step_id] = receipt
                logger.info(
                    "Signed Audit Receipt generated",
                    step_id=step_id,
                    agent_name=agent_type,
                    input_hash=in_hash,
                    output_hash=out_hash,
                    parent_receipt_id=parent_id
                )
                
                # Success: reset circuit breaker
                self.circuit_breaker.record_success(agent_type)
                metrics.end_step(step_id)
                return output
            except Exception as e:
                if isinstance(e, asyncio.TimeoutError):
                    e = TransientError(f"Step timed out after {step.timeout}s")
                
                # Failure: record to circuit breaker
                self.circuit_breaker.record_failure(agent_type)
                metrics.record_error(step_id, str(e))
                
                # Determine strategy from failure handler
                strategy, fallback_agent = self.failure_handler.determine_strategy(step, e, attempt - 1, current_agent=agent_type)
                
                if strategy == "retry":
                    metrics.record_retry(step_id)
                    delay = step.retry_policy.initial_delay * (step.retry_policy.backoff_factor ** (attempt - 1))
                    await queue.put(OutputChunk(
                        step_id=step_id,
                        content=f"⚠️ Step failed: {str(e)}. Retrying in {delay:.2f}s...",
                        status="retry"
                    ))
                    await asyncio.sleep(delay)
                    attempt += 1
                elif strategy == "degrade":
                    # Return best-effort degraded output
                    warning_msg = f"⚠️ Step degraded: {str(e)}"
                    await queue.put(OutputChunk(
                        step_id=step_id,
                        content=warning_msg,
                        status="warning"
                    ))
                    return {
                        "status": "degraded",
                        "warning": warning_msg,
                        "results": []
                    }
                elif strategy == "fallback" and fallback_agent:
                    await queue.put(OutputChunk(
                        step_id=step_id,
                        content=f"⚠️ Swapping to fallback agent '{fallback_agent}' due to error: {str(e)}",
                        status="warning"
                    ))
                    agent_type = fallback_agent
                    attempt = 1 # Reset attempt count for the fallback agent
                else:
                    # Escalate (halt DAG execution)
                    await queue.put(OutputChunk(
                        step_id=step_id,
                        content=f"❌ Step failed critically: {str(e)}",
                        status="failed"
                    ))
                    raise e
