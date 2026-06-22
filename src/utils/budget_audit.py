import asyncio
import json
import time
from hashlib import sha256
from typing import Any, Optional
from pydantic import BaseModel, Field

class BudgetExhausted(Exception):
    """Exception raised when execution cost exceeds the allowed budget limit."""
    pass

class BudgetTracker:
    """
    BudgetTracker enforces resource spending limits on agent invocations,
    preventing cost explosions and runaway loops.
    """
    
    def __init__(self, max_cost_cents: int = 100) -> None:
        self.max_cost = max_cost_cents
        self.spent = 0
        self.lock = asyncio.Lock()

    async def charge(self, cost_cents: int, operation: str) -> bool:
        """
        Deducts cost from the budget. 
        Raises BudgetExhausted if cost exceeds the remaining budget limit.
        """
        async with self.lock:
            if self.spent + cost_cents > self.max_cost:
                raise BudgetExhausted(
                    f"Cost budget exhausted: spent {self.spent}¢, charging {cost_cents}¢ "
                    f"for '{operation}' exceeds limit of {self.max_cost}¢."
                )
            self.spent += cost_cents
            return True

class AuditReceipt(BaseModel):
    """
    Represent a signed tamper-evident step verification.
    Provides step-level audit trails and chain of custody tracking.
    """
    step_id: str
    agent_name: str
    input_hash: str
    output_hash: str
    timestamp: float = Field(default_factory=time.time)
    parent_receipt_id: Optional[str] = None  # Link to preceding step's output hash

def generate_hash(data: Any) -> str:
    """Generates a SHA-256 hash of a serializable object for receipt generation."""
    try:
        serialized = json.dumps(data, sort_keys=True)
    except Exception:
        serialized = str(data)
    return sha256(serialized.encode("utf-8")).hexdigest()
