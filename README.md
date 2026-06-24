# Multi-Agent Orchestration System

This project is a multi-agent orchestration system built from scratch in Python 3.11+. It is designed to demonstrate key system programming patterns such as concurrency control, manual DAG execution, rate limiting/batching, circuit breaking, and backpressure protection without relying on third-party frameworks like LangChain or CrewAI.

---

## Architecture

The system uses a custom Directed Acyclic Graph (DAG) execution engine that validates dependencies using Kahn's algorithm and schedules concurrent task execution when prerequisites are met.

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

## Core Features

- **DAG Execution Engine**: Uses Kahn's algorithm for topological sorting and schedules tasks concurrently using `asyncio`.
- **Query Batcher**: Groups concurrent retrieval queries. If one query fails, the rest in the batch continue executing.
- **Circuit Breakers & Fallbacks**: Automatically trips after 3 consecutive failures, routes requests to a fallback agent, and attempts recovery after a timeout.
- **Backpressure Protection**: Bounded queue limits (`maxsize=100`) to manage memory usage under high load.
- **SSE Connection Management**: Automatically cancels background execution if the client disconnects.

---

## Quick Start

### 1. Environment Setup
```bash
git clone <repository_url> multi-agent-orchestrator
cd multi-agent-orchestrator
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Start the API Server
Start the local FastAPI server:
```bash
PYTHONPATH=. ./venv/bin/python -m uvicorn src.main:app --reload --port 8000
```
The server exposes a streaming Server-Sent Events (SSE) API at `/api/orchestrate`.

### 3. Run the CLI Demo
To run the pre-configured scenarios (happy path, fallback recovery, and parallel analysis):
```bash
PYTHONPATH=. ./venv/bin/python scripts/demo.py
```

---

## Running Tests
Run the test suite using pytest to verify system functionality and coverage:
```bash
PYTHONPATH=. ./venv/bin/pytest --cov=src --cov-branch --cov-report=term-missing tests/
```

---

## Docker Build
To build and run the application inside a Docker container:
```bash
docker build -t multi-agent-orchestrator .
docker run -p 8000:8000 multi-agent-orchestrator
```

---

## Known Issues

- **Wildcard CORS**: CORS is configured with wildcards (`"*"`) for local development convenience.
- **Demo Script Interrupts**: Terminating the CLI demo with Ctrl+C may experience a delay while background batcher tasks finish.
- **State Persistence**: Active execution state is stored in-memory and will reset if the server restarts.
