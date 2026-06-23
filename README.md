# Multi-Agent Orchestrator (from scratch!)

Hey! This is my project for building a multi-agent orchestration system entirely from scratch in Python 3.11+. I didn't use any black-box frameworks like LangChain, CrewAI, or networkx because I wanted to learn how concurrency, rate limiting, and DAG executors actually work under the hood.

---

## What this thing actually does

The core of the system is a manual DAG execution engine. It validates step relationships using Kahn's algorithm (to catch cycle loops before they run) and schedules tasks in parallel when their dependencies are met.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Prompt Input → [Parser Agent] → [Planner Agent] → [DAG Executor]            │
│                                                          ↓                  │
│                                                 ┌────────┴────────┐         │
│                                                 ↓                 ↓         │
│                                            [Retriever]       [Analyzer]     │
│                                                 ↓                 ↓         │
│                                                 └────────┬────────┘         │
│                                                          ↓                  │
│                                                    [Writer Agent]           │
│                                                          ↓                  │
│                                                    SSE output stream        │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Features I implemented

- **Manual DAG Executor**: Kahn's algorithm topological sorter that schedules task execution using `asyncio.create_task` and `asyncio.gather`.
- **First-Principles Batcher**: Groups concurrent database/retrieval queries. If one query fails in the batch, the rest keep executing instead of crashing the whole batch.
- **Circuit Breakers & Fallbacks**: Manual circuit breakers trip after 3 failures. Swaps execution to a fallback agent automatically if the primary circuit is open.
- **Backpressure Protection**: Bounded queue limits (`maxsize=100`) prevent the server from running out of memory if the client is slow to read the stream.
- **SSE Connection Management**: Cancels the background task automatically if the client disconnects, preventing orphaned LLM runs.
- **Interactive Sandbox Dashboard**: A dark-mode, glassmorphic UI served directly at the root URL `/` to let you inject failures and visualize DAG execution in real-time.

---

## 🚀 Quick Start (Get it running in 2 mins)

### Step 1: Set up the environment
```bash
git clone <repository_url> multi-agent-orchestrator
cd multi-agent-orchestrator
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Step 2: Run the Real-Time Sandbox Dashboard (Highly Recommended!)
Start the local server:
```bash
PYTHONPATH=. ./venv/bin/python -m uvicorn src.main:app --reload --port 8000
```
Then open your browser to [http://localhost:8000/](http://localhost:8000/).
Tbh, this is the coolest part of the project. You can type prompts, watch the DAG nodes execute live, click "Inject Retriever Failure" to trip the circuit breaker, and see the system self-heal via fallback agents in real-time.

### Step 3: Run the CLI Demo
This runs 3 scenarios: Scenario A (happy path), Scenario B (failure and degraded fallback recovery), and Scenario C (parallel climate policies analysis).
```bash
PYTHONPATH=. ./venv/bin/python scripts/demo.py
```

---

## 🧪 Running Tests
We have a full test suite with 30 unit/integration tests achieving **91% statement coverage**:
```bash
PYTHONPATH=. ./venv/bin/pytest --cov=src --cov-branch --cov-report=term-missing tests/
```

---

## 🐳 Docker Build
If you prefer running inside Docker:
```bash
docker build -t multi-agent-orchestrator .
docker run -p 8000:8000 multi-agent-orchestrator
```

---

## ⚠️ Known Issues & Bugs (that I know of)

- **Wildcard CORS**: CORS is set to wildcard `"*"` in main.py because I was having issues getting env variables loaded inside the docker container on my local machine. Will fix this later.
- **Ctrl+C Hangs**: If you Ctrl+C the demo script, it sometimes hangs for a second while the batcher flusher background tasks are cancelled. Just kill it using `kill -9` if it gets annoying.
- **No Persistence**: Everything is in-memory. If Uvicorn restarts, active execution states are gone.

*Built with a lot of coffee.*
