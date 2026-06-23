import json
from fastapi.testclient import TestClient
from src.main import app

def test_api_orchestrate_happy_path() -> None:
    """Verifies that the /api/orchestrate endpoint returns a valid SSE stream on happy path."""
    with TestClient(app) as client:
        payload = {
            "prompt": "Analyze recent AI breakthroughs in protein folding, summarize sentiment, and write a blog post outline"
        }
        response = client.post("/api/orchestrate", json=payload)
        assert response.status_code == 200
        assert "text/event-stream" in response.headers["content-type"]
        
        # Read the SSE stream lines
        lines = response.content.decode().split("\n\n")
        # Filter out empty lines
        data_lines = [line for line in lines if line.startswith("data:")]
        
        assert len(data_lines) > 0
        
        # Parse first chunk (should be parser output)
        first_chunk = json.loads(data_lines[0][5:])
        assert first_chunk["step_id"] == "parser"
        assert first_chunk["status"] == "completed"
        
        # Parse second chunk (should be planner output)
        second_chunk = json.loads(data_lines[1][5:])
        assert second_chunk["step_id"] == "planner"
        assert second_chunk["status"] == "completed"

def test_api_orchestrate_invalid_request() -> None:
    """Verifies that the /api/orchestrate endpoint returns structured parser error for ambiguous inputs."""
    with TestClient(app) as client:
        payload = {
            "prompt": "short"
        }
        response = client.post("/api/orchestrate", json=payload)
        assert response.status_code == 200
        
        lines = response.content.decode().split("\n\n")
        data_lines = [line for line in lines if line.startswith("data:")]
        
        assert len(data_lines) == 1
        chunk = json.loads(data_lines[0][5:])
        assert chunk["step_id"] == "parser"
        assert chunk["status"] == "failed"
        assert "clarifying_question" in chunk["content"]
