import time
from typing import Dict, Any

class MetricsTracker:
    """
    Should probably use Prometheus or OpenTelemetry for this, but didn't have time. 
    A simple dict-based tracker works fine for local profiling.
    """
    
    def __init__(self) -> None:
        self.start_time = time.time()
        self.step_latencies: Dict[str, float] = {}
        self.step_start_times: Dict[str, float] = {}
        self.step_retries: Dict[str, int] = {}
        self.errors: Dict[str, str] = {}
        
    def start_step(self, step_id: str) -> None:
        self.step_start_times[step_id] = time.time()
        
    def end_step(self, step_id: str) -> None:
        if step_id in self.step_start_times:
            latency = time.time() - self.step_start_times[step_id]
            self.step_latencies[step_id] = round(latency, 3)
            
    def record_retry(self, step_id: str) -> None:
        self.step_retries[step_id] = self.step_retries.get(step_id, 0) + 1
        
    def record_error(self, step_id: str, error_msg: str) -> None:
        self.errors[step_id] = error_msg
        
    def get_summary(self) -> Dict[str, Any]:
        end_time = time.time()
        total_duration = round(end_time - self.start_time, 3)
        return {
            "total_duration_sec": total_duration,
            "step_latencies_sec": self.step_latencies,
            "step_retries": self.step_retries,
            "errors": self.errors
        }
