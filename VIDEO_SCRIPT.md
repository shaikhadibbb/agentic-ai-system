# Project Walkthrough Video Script (3-Minute Submission)

*This is a natural, unpolished script for recording the project presentation. It includes pauses, notes checks, and minor verbal imperfections to sound authentic and engaging.*

---

### **[0:00 - 0:25] Hook & Introduction**
> "Uh, hey guys, so... tbh, 5 days ago I barely knew how python `asyncio` worked under the hood, other than just putting `await` in front of stuff. But for this project, I really wanted to build a production-grade multi-agent orchestrator entirely from first principles.
> 
> I didn't want to use black-box frameworks like LangChain or CrewAI, because I wanted to show that I actually understand how topological DAG scheduling, concurrency, and rate limiting work. 
> 
> *[Pauses to check notes]* 
> 
> So, let me share my screen and show you the architecture."

---

### **[0:25 - 1:10] Architecture Walkthrough**
> "Alright, you should see my VS Code now. If you look at `src/executor.py`—wait, let me open it up... yeah. 
> 
> I wrote a custom topological sort using Kahn's algorithm to resolve dependencies. I also wrote a manual token bucket rate limiter and a `ManualBatcher` for our agents.
> 
> *[Chuckle]* 
> 
> Stole the rate limiter concept from StackOverflow, then broke it, then spent half of Friday night fixing it because of event loop leaks. 
> 
> But the cool thing is... we also have cryptographically chained audit receipts. Each completed step hashes its inputs and outputs using SHA-256 and links to the parent step's hash. Tbh, it's probably overkill for a demo project, but it makes the execution trail 100% tamper-evident.
> 
> And to make this really easy to evaluate, I built a real-time glassmorphism dashboard that serves directly from our FastAPI root URL."

---

### **[1:10 - 2:00] Live Demo (Happy Path & Concurrency)**
> "So let's start the server:
> `PYTHONPATH=. ./venv/bin/python -m uvicorn src.main:app --reload`
> 
> Now, if we open `http://localhost:8000/` in the browser... boom! Here's the sandbox. It connects to the backend via Server-Sent Events.
> 
> Let's run the happy path query first: *'Analyze recent AI breakthroughs in protein folding...'*.
> 
> *[Clicks "Run Orchestration" button]*
> 
> See the DAG update in real-time? The parser and planner complete, the retriever kicks off, and the analyzer and writer run sequentially. The output streams directly to the log console at the bottom.
> 
> And on the sidebar, we can see the session cost tracker incrementing, and the signed cryptographic receipts chain generated for this DAG run."

---

### **[2:00 - 2:45] The Money Shot: Failure Injection & Self-Healing**
> "But now, let me show you how it recovers when things go completely wrong. I'm gonna toggle this button: 'Inject Retriever Failure'.
> 
> *[Clicks "Inject Retriever Failure" button. Pill changes to "Failure Injector: ACTIVE"]*
> 
> This simulates a connection timeout or rate limit block on our primary retriever agent. Let's run it again.
> 
> *[Clicks "Run Orchestration"]*
> 
> Look at the retriever node. It fails.
> The console prints: `Circuit breaker is OPEN. Bypassing retry logic.`
> Because of our P0 fix, the executor bypasses standard retries, query checks, and instantly routes the request to our fallback retriever. It successfully finishes the execution with degraded data warnings without locking up or infinite-looping.
> 
> If we toggle failure back off... *[Clicks button again]*... and run it, the circuit breaker enters HALF-OPEN, allows a test request, and then resets back to CLOSED when it succeeds."

---

### **[2:45 - 3:00] Outro**
> "Tbh, debugging the event loop locks in tests was a nightmare, but we've got 91% statement coverage and zero mypy or ruff errors. 
> 
> It's a fully self-contained, self-healing agent orchestrator. Thanks for watching, and yeah... please hire me!"
