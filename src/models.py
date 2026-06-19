# pydantic is magic but strict mode made me cry. Just using standard pydantic BaseModels here
# to validate incoming requests and data structures. Keeps typing clean.
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
import time

class TaskRequest(BaseModel):
    """Structured request representation after Task Parser Agent analysis."""
    raw_query: str = Field(..., description="Original raw request string from the user")
    intent: str = Field(..., description="Classified intent, e.g., retrieval, analysis, writing, or invalid")
    entities: List[str] = Field(default_factory=list, description="Extracted key entities, keywords, or topics")
    is_valid: bool = Field(default=True, description="Indicates if the query is clear enough to formulate a plan")
    ambiguity_explanation: Optional[str] = Field(default=None, description="Explanation if query is invalid or ambiguous")
    clarifying_question: Optional[str] = Field(default=None, description="Clarifying question if query is invalid or ambiguous")

class RetryPolicy(BaseModel):
    """Defines retry behavior for execution steps."""
    max_retries: int = Field(default=3, ge=0)
    backoff_factor: float = Field(default=1.5, ge=1.0)
    initial_delay: float = Field(default=0.5, ge=0.0)

class Step(BaseModel):
    """Represents an execution node (step) in the DAG."""
    step_id: str = Field(..., description="Unique identifier for the step")
    agent_type: str = Field(..., description="Type of agent assigned, e.g., retriever, analyzer, writer")
    inputs: Dict[str, Any] = Field(default_factory=dict, description="Input mappings for the step")
    outputs: Dict[str, Any] = Field(default_factory=dict, description="Captured outputs after execution")
    retry_policy: RetryPolicy = Field(default_factory=RetryPolicy)
    timeout: float = Field(default=10.0, description="Step timeout in seconds")
    dependencies: List[str] = Field(default_factory=list, description="List of step_ids this step depends on")

class ExecutionPlan(BaseModel):
    """The directed acyclic graph of steps representing the plan."""
    steps: Dict[str, Step] = Field(..., description="Map of step_id to Step")
    is_valid: bool = Field(default=True)
    validation_errors: List[str] = Field(default_factory=list)

class RetrievalResult(BaseModel):
    """Results returned by the Retriever Agent."""
    query: str
    source: str
    content: str
    confidence: float = Field(..., ge=0.0, le=1.0)

class AnalysisReport(BaseModel):
    """Results returned by the Analyzer Agent."""
    sentiment: str
    contradictions: List[str] = Field(default_factory=list)
    summary: str
    confidence: float = Field(..., ge=0.0, le=1.0)
    warning: Optional[str] = None

class WriterResponse(BaseModel):
    """Results returned by the Writer Agent."""
    markdown_content: str
    json_content: Optional[Dict[str, Any]] = None
    citations: List[Dict[str, Any]] = Field(default_factory=list)

class OutputChunk(BaseModel):
    """A streaming notification message emitted during step execution."""
    step_id: str
    content: Any
    status: str = Field(..., description="pending, running, completed, failed, warning, retry")
    timestamp: float = Field(default_factory=time.time)

class ExecutionState(BaseModel):
    """Preserves state of the executor for persistence and resume capabilities."""
    execution_id: str
    plan: ExecutionPlan
    step_states: Dict[str, Dict[str, Any]] = Field(default_factory=dict, description="step_id -> {status, input, output, retries_taken, error}")
    is_completed: bool = Field(default=False)
    timestamp: float = Field(default_factory=time.time)
