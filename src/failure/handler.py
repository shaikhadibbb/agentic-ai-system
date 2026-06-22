import time
import os
import json
from typing import Dict, Any, Tuple, Optional
from src.models import Step
from src.utils.logger import get_logger
from src.failure.circuit_breaker import CircuitBreakerOpenException

logger = get_logger("failure_handler")

class TransientError(Exception):
    """Exception indicating a temporary issue that might succeed on retry (e.g. rate limit, timeout)."""
    pass

class UnrecoverableError(Exception):
    """Exception indicating a fatal issue (e.g. syntax, permission, invalid config)."""
    pass

class DeadLetterQueue:
    """We write permanently failed runs here so we can debug later when things inevitably break in production."""
    
    def __init__(self, dlq_path: str = "/Users/adib/Desktop/Agentic Ai system/multi-agent-orchestrator/state/dlq.jsonl") -> None:
        self.dlq_path = dlq_path
        os.makedirs(os.path.dirname(dlq_path), exist_ok=True)

    def write(self, step_id: str, agent_type: str, inputs: Dict[str, Any], error: str) -> None:
        """Dumps the failure info into a local file. Keeps it simple without bringing in Kafka or RabbitMQ."""
        entry = {
            "timestamp": time.time(),
            "step_id": step_id,
            "agent_type": agent_type,
            "inputs": inputs,
            "error": error
        }
        try:
            with open(self.dlq_path, "a") as f:
                f.write(json.dumps(entry) + "\n")
            logger.info("Recorded step in Dead Letter Queue (DLQ)", step_id=step_id, dlq_path=self.dlq_path)
        except Exception as e:
            logger.error("Failed to write to DLQ", error=str(e))

class FailureHandler:
    """Decides what to do when an agent fails: try again, fall back to a cheaper agent, return partial data, or blow up."""

    def __init__(self, dlq: Optional[DeadLetterQueue] = None) -> None:
        self.dlq = dlq or DeadLetterQueue()

    def determine_strategy(
        self, 
        step: Step, 
        error: Exception, 
        retries_taken: int,
        current_agent: Optional[str] = None
    ) -> Tuple[str, Optional[str]]:
        """
        Figuring out what strategy to take. I check circuit breakers, unrecoverable errors, and retry limits.
        """
        logger.info(
            "Determining failure strategy", 
            step_id=step.step_id, 
            error_class=error.__class__.__name__,
            retries_taken=retries_taken,
            current_agent=current_agent
        )
        
        active_agent = current_agent or step.agent_type
        
        # 1. Circuit breaker is tripped, so don't bother retrying the dead agent. Go straight to fallback or degrade.
        if isinstance(error, CircuitBreakerOpenException):
            logger.warn("Circuit breaker is OPEN. Bypassing retry logic.", step_id=step.step_id, agent=active_agent)
            if active_agent == step.agent_type:
                fallback_agent = self._get_fallback_agent(step.agent_type)
                if fallback_agent:
                    return "fallback", fallback_agent
            
            # If fallback already failed/tripped or no fallback exists, degrade or escalate
            if step.agent_type == "retriever":
                queries = step.inputs.get("queries", [])
                if isinstance(queries, list) and len(queries) > 1:
                    return "degrade", None
                    
            self.dlq.write(step.step_id, active_agent, step.inputs, f"Circuit breaker OPEN: {str(error)}")
            return "escalate", None

        # 2. Some errors (like syntax or config issues) will never succeed, so fail immediately instead of wasting budget.
        if isinstance(error, UnrecoverableError):
            logger.error("Unrecoverable error encountered. Escalating.", step_id=step.step_id)
            self.dlq.write(step.step_id, active_agent, step.inputs, str(error))
            return "escalate", None
            
        # 3. Let's retry if we still have attempts left. Uses exponential backoff to not hammer the API.
        if retries_taken < step.retry_policy.max_retries:
            logger.info(
                "Attempting Retry", 
                step_id=step.step_id, 
                next_attempt=retries_taken + 1, 
                max_retries=step.retry_policy.max_retries
            )
            return "retry", None
            
        # We ran out of retries. Now we have to make a tough choice: degrade, fallback, or escalate.
        
        # 4. Check if Degrade is possible
        if step.agent_type == "retriever":
            queries = step.inputs.get("queries", [])
            if isinstance(queries, list) and len(queries) > 1:
                logger.warn("Retries exhausted. Choosing DEGRADE strategy to return best-effort data.", step_id=step.step_id)
                return "degrade", None
                
        # 5. Check if Fallback agent is available (only if we haven't already fallen back)
        if active_agent == step.agent_type:
            fallback_agent = self._get_fallback_agent(step.agent_type)
            if fallback_agent:
                logger.warn("Retries exhausted. Choosing FALLBACK strategy.", step_id=step.step_id, fallback_agent=fallback_agent)
                return "fallback", fallback_agent
            
        # 6. Otherwise, Escalate (halt everything)
        logger.error("Retries exhausted and no degrade/fallback possible. Escalating failure.", step_id=step.step_id)
        self.dlq.write(step.step_id, active_agent, step.inputs, f"Retries exhausted: {str(error)}")
        return "escalate", None

    def _get_fallback_agent(self, agent_type: str) -> Optional[str]:
        """Maps an agent type to its fallback backup counterpart."""
        fallback_map = {
            "retriever": "fallback_retriever",
            "analyzer": "fallback_analyzer"
        }
        return fallback_map.get(agent_type)
