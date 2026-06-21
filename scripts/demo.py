import asyncio
import uuid
import sys
from typing import Dict, Any

# Ensure project root is in python path
sys.path.append("/Users/adib/Desktop/Agentic Ai system/multi-agent-orchestrator")

from src.parser import TaskParserAgent
from src.planner import PlannerAgent
from src.executor import DAGExecutor
from src.agents.retriever import RetrieverAgent
from src.agents.analyzer import AnalyzerAgent
from src.agents.writer import WriterAgent
from src.failure.handler import FailureHandler
from src.failure.circuit_breaker import CircuitBreaker
from src.failure.state_manager import StateManager
from src.utils.logger import correlation_id

# Setup color helper
class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

async def consume_queue(queue: asyncio.Queue, task: asyncio.Task) -> Dict[str, Any]:
    """Helper to consume queue chunks in real-time and format CLI outputs."""
    final_output = {}
    
    while True:
        try:
            chunk = await asyncio.wait_for(queue.get(), timeout=0.1)
            
            step_id = chunk.step_id
            status = chunk.status
            content = chunk.content
            
            # Format logs according to chunk status
            if step_id == "parser":
                print(f"{Colors.OKCYAN}[PARSER]{Colors.ENDC} Intent: '{content.get('intent')}', Entities: {content.get('entities')}")
            elif step_id == "planner":
                print(f"{Colors.OKCYAN}[PLANNER]{Colors.ENDC} Formulated {len(content.get('steps', {}))} execution step(s) in DAG.")
            elif step_id == "system_metrics":
                print(f"{Colors.HEADER}[METRICS]{Colors.ENDC} Total Duration: {content.get('total_duration_sec')}s | Step Latencies: {content.get('step_latencies_sec')}")
            elif step_id == "final_output":
                final_output = content
            else:
                # Step-level agent executions
                if status == "running":
                    # Avoid repeating raw text chunks if it's the writer streaming
                    if step_id == "synthesize_report" or step_id == "write_response":
                        print(content, end="", flush=True)
                    else:
                        print(f"{Colors.BOLD}[RUNNING]{Colors.ENDC} Step '{step_id}': {content}")
                elif status == "completed":
                    print(f"\n{Colors.OKGREEN}[SUCCESS]{Colors.ENDC} Step '{step_id}': {content} ✓")
                elif status == "warning":
                    print(f"\n{Colors.WARNING}[WARNING]{Colors.ENDC} Step '{step_id}': {content} ⚠️")
                elif status == "retry":
                    print(f"\n{Colors.WARNING}[RETRY]{Colors.ENDC} Step '{step_id}': {content} 🔄")
                elif status == "failed":
                    print(f"\n{Colors.FAIL}[FAILED]{Colors.ENDC} Step '{step_id}': {content} ❌")
                    
            queue.task_done()
        except asyncio.TimeoutError:
            if task.done():
                # Drain remaining
                while not queue.empty():
                    chunk = queue.get_nowait()
                    step_id = chunk.step_id
                    if step_id == "final_output":
                        final_output = chunk.content
                    queue.task_done()
                break
                
    # Await task to raise exceptions if any occurred
    try:
        await task
    except Exception as e:
        print(f"\n{Colors.FAIL}[CRITICAL HALT]{Colors.ENDC} Pipeline failed with: {str(e)}")
        
    return final_output

async def run_scenario(name: str, query: str, executor: DAGExecutor, parser: TaskParserAgent, planner: PlannerAgent):
    print(f"\n{'='*80}")
    print(f"{Colors.HEADER}{Colors.BOLD}RUNNING SCENARIO: {name}{Colors.ENDC}")
    print(f"{Colors.BOLD}Input Request:{Colors.ENDC} \"{query}\"")
    print(f"{'='*80}\n")
    
    execution_id = str(uuid.uuid4())
    correlation_id.set(execution_id)
    
    # Reset budget spent for each scenario run
    from src.utils.budget_audit import BudgetTracker
    executor.budget_tracker = BudgetTracker(max_cost_cents=200)
    
    # 1. Parse Request
    task_req = parser.parse(query)
    
    # 2. Generate Plan
    plan = planner.create_plan(task_req)
    if not plan.is_valid:
        print(f"{Colors.FAIL}Formulating plan failed: {plan.validation_errors}{Colors.ENDC}")
        return
        
    # 3. Execute plan with real-time stream consumption
    queue = asyncio.Queue()
    task = asyncio.create_task(executor.execute(plan, queue, execution_id))
    
    # Consume queue
    await consume_queue(queue, task)
    
    # Get final result from task result dictionary
    final_result = None
    if task.exception() is None:
        outputs = task.result()
        for sid, step in plan.steps.items():
            if step.agent_type == "writer":
                final_result = outputs.get(sid)
                break
        if not final_result:
            for sid, step in plan.steps.items():
                if step.agent_type == "analyzer":
                    final_result = outputs.get(sid)
                    break
        if not final_result:
            final_result = outputs
            
    print(f"\n{Colors.OKGREEN}{Colors.BOLD}FINAL OUTPUT REPORT:{Colors.ENDC}")
    if final_result:
        if isinstance(final_result, dict) and "markdown_content" in final_result:
            print(final_result["markdown_content"])
        else:
            print(final_result)
    else:
        print(f"{Colors.FAIL}No output report generated (execution failed).{Colors.ENDC}")

async def main():
    import argparse
    parser_args = argparse.ArgumentParser(description="Multi-Agent Orchestration Demo")
    parser_args.add_argument("--scenario", type=str, choices=["A", "B", "C"], help="Run single scenario (A, B, or C)")
    args = parser_args.parse_args()

    # Setup instances
    retriever_primary = RetrieverAgent(batch_size=4, max_wait_ms=200.0)
    retriever_fallback = RetrieverAgent(batch_size=2, max_wait_ms=100.0)
    analyzer_primary = AnalyzerAgent(confidence_threshold=0.90)
    analyzer_fallback = AnalyzerAgent(confidence_threshold=0.70)
    writer_agent = WriterAgent()

    agents_registry = {
        "retriever": retriever_primary,
        "fallback_retriever": retriever_fallback,
        "analyzer": analyzer_primary,
        "fallback_analyzer": analyzer_fallback,
        "writer": writer_agent
    }

    failure_handler = FailureHandler()
    circuit_breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=5.0)
    state_manager = StateManager()

    executor = DAGExecutor(
        agents=agents_registry,
        failure_handler=failure_handler,
        circuit_breaker=circuit_breaker,
        state_manager=state_manager
    )

    parser = TaskParserAgent()
    planner = PlannerAgent()
    
    try:
        if not args.scenario or args.scenario == "A":
            # Scenario A: Happy Path
            await run_scenario(
                "SCENARIO A: Happy Path (Protein Folding)",
                "Analyze recent AI breakthroughs in protein folding, summarize sentiment, and write a blog post outline",
                executor, parser, planner
            )
            
        if not args.scenario or args.scenario == "B":
            # Scenario B: Failure + Recovery (Stock prices batching & invalid ticket)
            await run_scenario(
                "SCENARIO B: Failure + Recovery (Stock Prices)",
                "Retrieve stock prices for AAPL, GOOGL, INVALID_TICKET, MSFT and analyze trends",
                executor, parser, planner
            )
            
        if not args.scenario or args.scenario == "C":
            # Scenario C: Parallel Retrievals & Sequential Analysis
            await run_scenario(
                "SCENARIO C: Complex Multi-Step Parallel execution",
                "Compare climate policies of EU, USA, and China from 2023-2024. Identify contradictions. Write a balanced report.",
                executor, parser, planner
            )
            
    finally:
        # Shutdown batchers
        await retriever_primary.batcher.shutdown()
        await retriever_fallback.batcher.shutdown()

if __name__ == "__main__":
    main_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(main_loop)
    try:
        main_loop.run_until_complete(main())
    finally:
        main_loop.close()
