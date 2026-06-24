import asyncio
import uuid
import os
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Optional, Dict
from fastapi import FastAPI
from fastapi.responses import StreamingResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from src.models import OutputChunk
from src.parser import TaskParserAgent
from src.planner import PlannerAgent
from src.executor import DAGExecutor
from src.agents.retriever import RetrieverAgent
from src.agents.analyzer import AnalyzerAgent
from src.agents.writer import WriterAgent
from src.failure.handler import FailureHandler
from src.failure.circuit_breaker import CircuitBreaker
from src.failure.state_manager import StateManager
from src.utils.logger import get_logger, correlation_id

logger = get_logger("fastapi_app")

# Define Agent Instances and Registry
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

# Failure Handling Infrastructure
failure_handler = FailureHandler()
circuit_breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=5.0)
state_manager = StateManager()

executor = DAGExecutor(
    agents=agents_registry,
    failure_handler=failure_handler,
    circuit_breaker=circuit_breaker,
    state_manager=state_manager
)

parser_agent = TaskParserAgent()
planner_agent = PlannerAgent()

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    # Startup logic
    logger.info("Starting up FastAPI Multi-Agent Orchestration Server")
    yield
    # Shutdown logic: clean up manual batcher background flushers
    logger.info("Shutting down servers and manual batcher flushers")
    await retriever_primary.batcher.shutdown()
    await retriever_fallback.batcher.shutdown()

app = FastAPI(
    title="Multi-Agent Orchestration System API",
    description="A production-grade, DAG-based multi-agent executor built from scratch.",
    version="1.0.0",
    lifespan=lifespan
)

# CORS enablement
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class OrchestrationRequest(BaseModel):
    prompt: str

@app.api_route("/api/orchestrate", methods=["GET", "POST"])
async def orchestrate(req: Optional[OrchestrationRequest] = None, prompt: Optional[str] = None) -> StreamingResponse:
    """
    Triggers orchestration, returning SSE stream of active executions.
    """
    actual_prompt = prompt or (req.prompt if req else None)
    if not actual_prompt:
        async def missing_prompt_generator() -> AsyncGenerator[str, None]:
            err_chunk = OutputChunk(
                step_id="system",
                content={"error": "Missing prompt parameter"},
                status="failed"
            )
            yield f"data: {err_chunk.model_dump_json()}\n\n"
        return StreamingResponse(missing_prompt_generator(), media_type="text/event-stream")

    assert actual_prompt is not None
    execution_id = str(uuid.uuid4())
    correlation_id.set(execution_id)
    logger.info("Received orchestration request", prompt=actual_prompt, execution_id=execution_id)
    
    # 1. Parse raw query
    task_req = parser_agent.parse(actual_prompt)
    if not task_req.is_valid:
        # Query is ambiguous or invalid. Immediately return structured SSE error.
        async def invalid_generator() -> AsyncGenerator[str, None]:
            err_chunk = OutputChunk(
                step_id="parser",
                content={
                    "error": task_req.ambiguity_explanation,
                    "clarifying_question": task_req.clarifying_question
                },
                status="failed"
            )
            yield f"data: {err_chunk.model_dump_json()}\n\n"
        return StreamingResponse(invalid_generator(), media_type="text/event-stream")
        
    # 2. Plan execution steps
    plan = planner_agent.create_plan(task_req)
    if not plan.is_valid:
        async def invalid_plan_generator() -> AsyncGenerator[str, None]:
            err_chunk = OutputChunk(
                step_id="planner",
                content={"error": f"Failed to formulate execution plan: {', '.join(plan.validation_errors)}"},
                status="failed"
            )
            yield f"data: {err_chunk.model_dump_json()}\n\n"
        return StreamingResponse(invalid_plan_generator(), media_type="text/event-stream")

    # 3. Create execution queue (bounded for backpressure protection) and run executor task
    from src.utils.budget_audit import BudgetTracker
    req_budget = BudgetTracker(max_cost_cents=200)
    queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=100)
    task = asyncio.create_task(executor.execute(plan, queue, execution_id, budget_tracker=req_budget))
    
    async def sse_event_generator() -> AsyncGenerator[str, None]:
        # Emit initial parsed and planned states
        parser_done_chunk = OutputChunk(
            step_id="parser",
            content=task_req.model_dump(),
            status="completed"
        )
        yield f"data: {parser_done_chunk.model_dump_json()}\n\n"
        
        planner_done_chunk = OutputChunk(
            step_id="planner",
            content=plan.model_dump(),
            status="completed"
        )
        yield f"data: {planner_done_chunk.model_dump_json()}\n\n"
        
        try:
            while True:
                try:
                    # Wait briefly for items to stream
                    chunk = await asyncio.wait_for(queue.get(), timeout=0.1)
                    yield f"data: {chunk.model_dump_json()}\n\n"
                    queue.task_done()
                except asyncio.TimeoutError:
                    # Send periodic SSE ping comment to keep client connection alive
                    yield ": ping\n\n"
                    
                    if task.done():
                        # Drain remaining items if task completes
                        while not queue.empty():
                            chunk = queue.get_nowait()
                            yield f"data: {chunk.model_dump_json()}\n\n"
                            queue.task_done()
                        break
                        
            # Handle final response synthesis
            if task.exception() is not None:
                err = task.exception()
                error_chunk = OutputChunk(
                    step_id="system",
                    content={"error": str(err)},
                    status="failed"
                )
                yield f"data: {error_chunk.model_dump_json()}\n\n"
            else:
                outputs = task.result()
                
                # Retrieve final agent payload
                writer_output = None
                # Check for writer step output
                for sid, step in plan.steps.items():
                    if step.agent_type == "writer":
                        writer_output = outputs.get(sid)
                        break
                # If no writer, look for analyzer output (Scenario B)
                if not writer_output:
                    for sid, step in plan.steps.items():
                        if step.agent_type == "analyzer":
                            writer_output = outputs.get(sid)
                            break
                            
                final_chunk = OutputChunk(
                    step_id="final_output",
                    content=writer_output or outputs,
                    status="completed"
                )
                yield f"data: {final_chunk.model_dump_json()}\n\n"
                
        except Exception as ex:
            logger.error("SSE stream handler error", error=str(ex))
            error_chunk = OutputChunk(
                step_id="system",
                content={"error": str(ex)},
                status="failed"
            )
            yield f"data: {error_chunk.model_dump_json()}\n\n"
        finally:
            if not task.done():
                logger.warn("SSE connection closed while task was running. Cancelling task.", execution_id=execution_id)
                task.cancel()
                try:
                    await asyncio.wait_for(task, timeout=2.0)
                except Exception:
                    pass
            
    return StreamingResponse(sse_event_generator(), media_type="text/event-stream")

