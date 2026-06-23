import os
import json
from typing import Optional
from src.models import ExecutionState
from src.utils.logger import get_logger

logger = get_logger("state_manager")

class StateManager:
    """
    json files are ghetto but they work for persistent state storage. Keeps it super simple 
    without running a Postgres container during local development.
    """

    def __init__(self, storage_dir: str = "/Users/adib/Desktop/Agentic Ai system/multi-agent-orchestrator/state") -> None:
        self.storage_dir = storage_dir
        # Ensure directories exist
        try:
            os.makedirs(self.storage_dir, exist_ok=True)
        except Exception as e:
            logger.error("Failed to create storage directory", error=str(e))

    def save_state(self, state: ExecutionState) -> None:
        """Serializes and saves the execution state to a JSON file."""
        file_path = os.path.join(self.storage_dir, f"{state.execution_id}.json")
        try:
            with open(file_path, "w") as f:
                f.write(state.model_dump_json(indent=2))
            logger.info("Saved execution state", execution_id=state.execution_id, path=file_path)
        except Exception as e:
            logger.error("Failed to save execution state", execution_id=state.execution_id, error=str(e))

    def load_state(self, execution_id: str) -> Optional[ExecutionState]:
        """Loads and deserializes execution state from a JSON file."""
        file_path = os.path.join(self.storage_dir, f"{execution_id}.json")
        if not os.path.exists(file_path):
            logger.warn("State file not found", execution_id=execution_id)
            return None
            
        try:
            with open(file_path, "r") as f:
                data = json.load(f)
            state = ExecutionState.model_validate(data)
            logger.info("Loaded execution state", execution_id=execution_id)
            return state
        except Exception as e:
            logger.error("Failed to load execution state", execution_id=execution_id, error=str(e))
            return None
            
    def delete_state(self, execution_id: str) -> None:
        """Deletes execution state file."""
        file_path = os.path.join(self.storage_dir, f"{execution_id}.json")
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
                logger.info("Deleted state file", execution_id=execution_id)
            except Exception as e:
                logger.error("Failed to delete state file", execution_id=execution_id, error=str(e))
