import time
from typing import Dict
from src.utils.logger import get_logger

logger = get_logger("circuit_breaker")

class CircuitBreakerOpenException(Exception):
    """Exception raised when an operation is blocked by an open circuit breaker."""
    pass

class CircuitBreaker:
    """
    state machines are confusing so I kept this breaker simple with just a failure count dict 
    and cooldown timestamp checking. No bloated state-machine libraries needed here.
    """
    
    def __init__(self, failure_threshold: int = 3, recovery_timeout: float = 5.0) -> None:
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.state: Dict[str, str] = {}           # "CLOSED", "OPEN", "HALF-OPEN"
        self.failures: Dict[str, int] = {}
        self.last_failure_time: Dict[str, float] = {}

    def get_state(self, agent_type: str) -> str:
        """Determines and updates the state of the circuit breaker for an agent type."""
        current_state = self.state.get(agent_type, "CLOSED")
        
        if current_state == "OPEN":
            last_fail = self.last_failure_time.get(agent_type, 0.0)
            elapsed = time.time() - last_fail
            if elapsed > self.recovery_timeout:
                logger.info("Circuit breaker entering HALF-OPEN state", agent_type=agent_type, elapsed=elapsed)
                self.state[agent_type] = "HALF-OPEN"
                return "HALF-OPEN"
                
        return current_state

    def check(self, agent_type: str) -> None:
        """Checks if the circuit is open, raising an exception if it is."""
        state = self.get_state(agent_type)
        if state == "OPEN":
            raise CircuitBreakerOpenException(
                f"Circuit breaker is OPEN for agent type: {agent_type}. Cooldown in progress."
            )

    def record_success(self, agent_type: str) -> None:
        """Records a successful operation, closing the circuit."""
        old_state = self.state.get(agent_type, "CLOSED")
        self.failures[agent_type] = 0
        self.state[agent_type] = "CLOSED"
        if old_state != "CLOSED":
            logger.info("Circuit breaker reset to CLOSED", agent_type=agent_type)

    def record_failure(self, agent_type: str) -> None:
        """Records a failure. Trips to OPEN if threshold is crossed."""
        self.failures[agent_type] = self.failures.get(agent_type, 0) + 1
        self.last_failure_time[agent_type] = time.time()
        
        logger.warn(
            "Circuit breaker recorded failure", 
            agent_type=agent_type, 
            failure_count=self.failures[agent_type],
            threshold=self.failure_threshold
        )
        
        if self.failures[agent_type] >= self.failure_threshold:
            logger.error("Circuit breaker tripped to OPEN!", agent_type=agent_type)
            self.state[agent_type] = "OPEN"
