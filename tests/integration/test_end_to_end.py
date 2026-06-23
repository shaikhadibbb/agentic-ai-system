import pytest
import asyncio
from src.parser import TaskParserAgent
from src.planner import PlannerAgent
from src.executor import DAGExecutor
from src.agents.retriever import RetrieverAgent
from src.agents.analyzer import AnalyzerAgent
from src.agents.writer import WriterAgent
from src.failure.handler import FailureHandler
from src.failure.circuit_breaker import CircuitBreaker
from src.failure.state_manager import StateManager

@pytest.mark.asyncio
async def test_end_to_end_scenario_a_happy_path() -> None:
    # Setup
    retriever = RetrieverAgent(batch_size=4, max_wait_ms=10.0)
    analyzer = AnalyzerAgent()
    writer = WriterAgent()
    
    agents = {
        "retriever": retriever,
        "analyzer": analyzer,
        "writer": writer
    }
    
    executor = DAGExecutor(
        agents=agents,
        failure_handler=FailureHandler(),
        circuit_breaker=CircuitBreaker(),
        state_manager=StateManager()
    )
    
    parser = TaskParserAgent()
    planner = PlannerAgent()
    
    query = "Analyze recent AI breakthroughs in protein folding, summarize sentiment, and write a blog post outline"
    
    # Run
    task_req = parser.parse(query)
    assert task_req.is_valid is True
    
    plan = planner.create_plan(task_req)
    assert plan.is_valid is True
    
    queue = asyncio.Queue()
    execution_id = "test-e2e-a"
    
    outputs = await executor.execute(plan, queue, execution_id)
    await retriever.batcher.shutdown()
    
    assert "retrieve_data" in outputs
    assert "analyze_data" in outputs
    assert "write_response" in outputs
    
    # Assert final writer response has synthesized content and citations
    writer_out = outputs["write_response"]
    assert "markdown_content" in writer_out
    assert "AlphaFold 3" in writer_out["markdown_content"]
    assert "arXiv:2405.08805" in writer_out["markdown_content"]

@pytest.mark.asyncio
async def test_end_to_end_scenario_b_degradation() -> None:
    # Setup
    retriever = RetrieverAgent(batch_size=4, max_wait_ms=10.0)
    analyzer = AnalyzerAgent()
    writer = WriterAgent()
    
    agents = {
        "retriever": retriever,
        "analyzer": analyzer,
        "writer": writer
    }
    
    executor = DAGExecutor(
        agents=agents,
        failure_handler=FailureHandler(),
        circuit_breaker=CircuitBreaker(),
        state_manager=StateManager()
    )
    
    parser = TaskParserAgent()
    planner = PlannerAgent()
    
    # AAPL, GOOGL, MSFT should succeed; INVALID_TICKET will fail
    query = "Retrieve stock prices for AAPL, GOOGL, INVALID_TICKET, MSFT and analyze trends"
    
    # Run
    task_req = parser.parse(query)
    plan = planner.create_plan(task_req)
    
    queue = asyncio.Queue()
    execution_id = "test-e2e-b"
    
    outputs = await executor.execute(plan, queue, execution_id)
    await retriever.batcher.shutdown()
    
    assert "retrieve_stocks" in outputs
    assert "analyze_stocks" in outputs
    
    ret_out = outputs["retrieve_stocks"]
    assert ret_out["status"] == "degraded"
    assert len(ret_out["results"]) == 3 # AAPL, GOOGL, MSFT
    assert len(ret_out["errors"]) == 1 # INVALID_TICKET
    assert ret_out["errors"][0]["query"] == "INVALID_TICKET"
    
    ana_out = outputs["analyze_stocks"]
    assert "sentiment" in ana_out
    assert "degraded" in ana_out["warning"]

@pytest.mark.asyncio
async def test_end_to_end_scenario_c_parallel() -> None:
    # Setup
    retriever = RetrieverAgent(batch_size=4, max_wait_ms=10.0)
    analyzer = AnalyzerAgent()
    writer = WriterAgent()
    
    agents = {
        "retriever": retriever,
        "analyzer": analyzer,
        "writer": writer
    }
    
    executor = DAGExecutor(
        agents=agents,
        failure_handler=FailureHandler(),
        circuit_breaker=CircuitBreaker(),
        state_manager=StateManager()
    )
    
    parser = TaskParserAgent()
    planner = PlannerAgent()
    
    query = "Compare climate policies of EU, USA, and China from 2023-2024. Identify contradictions. Write a balanced report."
    
    # Run
    task_req = parser.parse(query)
    plan = planner.create_plan(task_req)
    
    queue = asyncio.Queue()
    execution_id = "test-e2e-c"
    
    outputs = await executor.execute(plan, queue, execution_id)
    await retriever.batcher.shutdown()
    
    # Verify EU, USA, China parallel retrievals and sequential analysis
    assert "retrieve_eu" in outputs
    assert "retrieve_usa" in outputs
    assert "retrieve_china" in outputs
    assert "analyze_eu" in outputs
    assert "analyze_usa" in outputs
    assert "analyze_china" in outputs
    assert "synthesize_report" in outputs
    
    # Assert final writer response has synthesized content and citations
    writer_out = outputs["synthesize_report"]
    assert "markdown_content" in writer_out
    assert "EU" in writer_out["markdown_content"]
    assert "USA" in writer_out["markdown_content"]
    assert "China" in writer_out["markdown_content"]
    assert "Contradiction" in writer_out["markdown_content"]
