from src.planner import PlannerAgent
from src.models import TaskRequest, ExecutionPlan, Step

def test_planner_scenario_a_generation() -> None:
    planner = PlannerAgent()
    req = TaskRequest(
        raw_query="Analyze recent AI breakthroughs in protein folding, summarize sentiment, and write a blog post outline",
        intent="orchestration",
        entities=["protein folding"],
        is_valid=True
    )
    plan = planner.create_plan(req)
    
    assert plan.is_valid is True
    assert len(plan.steps) == 3
    assert "retrieve_data" in plan.steps
    assert "analyze_data" in plan.steps
    assert "write_response" in plan.steps
    
    assert plan.steps["analyze_data"].dependencies == ["retrieve_data"]
    assert plan.steps["write_response"].dependencies == ["analyze_data"]

def test_planner_cycle_detection() -> None:
    planner = PlannerAgent()
    
    # Create invalid plan with cycle: stepA -> stepB -> stepA
    step_a = Step(
        step_id="stepA",
        agent_type="retriever",
        dependencies=["stepB"]
    )
    step_b = Step(
        step_id="stepB",
        agent_type="analyzer",
        dependencies=["stepA"]
    )
    
    plan = ExecutionPlan(steps={"stepA": step_a, "stepB": step_b})
    is_valid, errors = planner.validate_plan(plan)
    
    assert is_valid is False
    assert any("Cycle detected" in err for err in errors)

def test_planner_missing_dependency() -> None:
    planner = PlannerAgent()
    
    # Step A depends on Step C which is missing
    step_a = Step(
        step_id="stepA",
        agent_type="retriever",
        dependencies=["stepC"]
    )
    
    plan = ExecutionPlan(steps={"stepA": step_a})
    is_valid, errors = planner.validate_plan(plan)
    
    assert is_valid is False
    assert any("depends on non-existent step" in err for err in errors)
