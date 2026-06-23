# Recording Notes & Guide

These are quick notes to help you record the 3-minute project video smoothly.

---

## 1. Quick Setup & Checks
Before hitting record, run this single validation line in your terminal to ensure there are no lingering mypy/ruff complaints or failing tests:
```bash
PYTHONPATH=. ./venv/bin/pytest tests/ && PYTHONPATH=. ./venv/bin/mypy --strict src && ./venv/bin/ruff check .
```
Expected output:
- `30 passed`
- `Success: no issues found`
- `All checks passed!`

---

## 2. Running the Server & Dashboard
1. Spin up the FastAPI app locally:
   ```bash
   PYTHONPATH=. ./venv/bin/python -m uvicorn src.main:app --reload
   ```
2. Open your browser to:
   [http://localhost:8000/](http://localhost:8000/)
   *The page should load a dark-mode glassmorphic dashboard visualizing the DAG.*

---

## 3. Demo Walkthrough Steps

### Step 1: Happy Path
1. Keep the "Failure Injector" toggled **OFF**.
2. Click **Run Orchestration** (using the default Protein Folding prompt).
3. Verify:
   - The nodes change colors: `pending` (gray) -> `running` (blue/yellow) -> `completed` (green).
   - Factual summary text streams in the console log at the bottom.
   - Session cost increments.

### Step 2: Injecting Failure (The Self-Healing Demo)
1. Click the red **Inject Retriever Failure** button.
   - The indicator in the top right will change to `Failure Injector: ACTIVE`.
2. Click **Run Orchestration** again.
3. Verify:
   - The `retrieve_data` node fails (turns red/orange).
   - The console logs: `Circuit breaker is OPEN. Bypassing retry logic. Swapping to fallback retriever...`
   - The execution succeeds using the fallback agent and returns a degraded output status.
4. Click **Inject Retriever Failure** again to disable the mock failure.
5. Trigger one more run. Verify that the circuit transitions to `HALF-OPEN` then successfully resets to `CLOSED`.

---

## 4. Troubleshooting & Tips
- **If the dashboard hangs**: Close the browser tab, terminate the server (`ctrl+c`), clear any state files using `rm -rf state/*.json`, and start the server again.
- **If the circuit breaker doesn't trip**: Make sure you triggered the run *while* the Failure Injector was active. The Failure Injector forces the retriever agent to throw a transient error on every attempt, which increments the breaker's failure count and immediately trips it.
