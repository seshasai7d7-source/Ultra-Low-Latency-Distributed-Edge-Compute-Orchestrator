# Ultra-Low Latency Distributed Edge-Compute Orchestrator

![Python 3.11](https://img.shields.io/badge/Python-3.11-blue?logo=python&logoColor=white)
![asyncio](https://img.shields.io/badge/asyncio-native-green)
![FastAPI](https://img.shields.io/badge/FastAPI-0.100%2B-009688?logo=fastapi&logoColor=white)
![Pydantic v2](https://img.shields.io/badge/Pydantic-v2-e92063?logo=pydantic&logoColor=white)
![License MIT](https://img.shields.io/badge/License-MIT-yellow)

---

## Description

The **Ultra-Low Latency Distributed Edge-Compute Orchestrator** is a Python-based distributed task execution system designed for high-throughput, latency-sensitive edge-compute workloads. It follows a **single-master, multi-worker architecture** in which the master node manages the full task lifecycle — submission, validation, scheduling, dispatching, and result collection — while worker nodes execute task payloads in isolated process contexts. The master runs an `asyncio`/`uvloop` event loop for non-blocking concurrency and exposes a **FastAPI** REST API gateway with automatic OpenAPI documentation. All internal data models are defined with **Pydantic v2** for strict runtime validation and seamless JSON serialisation. The worker communication layer uses a **pluggable messaging interface** backed by a **ZeroMQ** implementation, allowing the transport layer to be swapped or stubbed without affecting orchestrator logic.

---

## Team

| Developer | Role | Owns |
|---|---|---|
| **Developer V** | Architecture, asyncio/uvloop, orchestrator core, scheduler, dispatcher, task tracking, API gateway, documentation | All orchestrator modules, models, API, docs |
| **Developer S** | ZeroMQ networking, communication layer, worker transport implementation | `messaging/` concrete implementation, networking environment |

---

## Features

- **Async-first architecture** — entire orchestrator built on Python `asyncio`; no blocking I/O on the event loop
- **High-performance uvloop event loop** — libuv-backed event loop on Linux/macOS for significantly reduced coroutine scheduling latency
- **Priority-based task scheduling** — `AsyncTaskQueue` with configurable priority bands and FIFO ordering within bands
- **9-state task lifecycle** — strict `PENDING → VALIDATED → QUEUED → DISPATCHED → RUNNING → COMPLETED / FAILED / TIMED_OUT / CANCELLED` with transition enforcement and retry support
- **FastAPI REST API** — automatic OpenAPI/Swagger UI at `/docs`, ReDoc at `/redoc`, versioned endpoints under `/api/v1/`
- **Pydantic v2 request/response validation** — schema enforcement at the API boundary; invalid requests rejected with structured `HTTP 422` before reaching the orchestrator
- **Pluggable messaging interface** — `MessagingInterface` Protocol defines the transport contract; ZeroMQ implementation provided by Developer S; `StubMessaging` available for development without ZeroMQ
- **Prometheus metrics and observability** — per-component metrics (task throughput, queue depth, worker utilisation, latency histograms) exposed at `/metrics`

---

## Project Structure

```
edge-orchestrator/
├── src/
│   └── edge_orchestrator/
│       ├── __init__.py
│       ├── main.py                    # Application bootstrap & entry point
│       ├── core/                      # Master orchestrator control plane
│       │   ├── __init__.py
│       │   ├── event_loop.py          # asyncio/uvloop platform detection
│       │   ├── orchestrator.py        # MasterOrchestrator coordinator
│       │   └── task_validator.py      # Pre-scheduling validation pipeline
│       ├── scheduler/                 # Scheduling subsystem
│       │   ├── __init__.py
│       │   ├── scheduler.py           # Priority-based TaskScheduler
│       │   └── task_queue.py          # Async-safe priority queue
│       ├── dispatcher/                # Dispatching subsystem
│       │   ├── __init__.py
│       │   └── dispatcher.py          # TaskDispatcher (worker routing & acks)
│       ├── tracking/                  # Lifecycle state machine
│       │   ├── __init__.py
│       │   └── task_tracker.py        # 9-state TaskTracker
│       ├── worker/                    # Worker node data plane
│       │   ├── __init__.py
│       │   ├── worker_manager.py      # Worker registry & health monitor
│       │   ├── worker_node.py         # WorkerNode daemon process
│       │   └── executor.py            # TaskExecutor child process runner
│       ├── security/                  # Security boundaries
│       │   ├── __init__.py
│       │   ├── ast_validator.py       # Static AST code analysis (pre-filter)
│       │   └── sandbox.py             # Process-level sandbox & rlimit policy
│       ├── messaging/                 # ZeroMQ integration boundary
│       │   ├── __init__.py
│       │   ├── interface.py           # MessagingInterface Protocol contract
│       │   └── stub.py                # StubMessaging for testing
│       ├── models/                    # Shared Pydantic v2 data models
│       │   ├── __init__.py
│       │   ├── task.py                # Task, TaskStatus, TaskAcknowledgement
│       │   ├── result.py              # TaskResult
│       │   └── worker.py              # WorkerInfo, WorkerStatus, HeartbeatMessage
│       ├── api/                       # FastAPI REST API gateway
│       │   ├── __init__.py
│       │   ├── server.py              # FastAPI app factory (create_app)
│       │   └── routes/                # Endpoint routers (tasks, workers, health)
│       ├── monitoring/                # Observability subsystem
│       │   ├── __init__.py
│       │   └── metrics.py             # Prometheus metrics registry
│       └── config/                    # Configuration management
│           ├── __init__.py
│           └── settings.py            # Pydantic Settings
├── tests/
│   ├── conftest.py                    # Shared pytest fixtures
│   ├── unit/
│   │   ├── test_ast_validator.py
│   │   ├── test_task_validator.py
│   │   ├── test_task_tracker.py
│   │   └── test_scheduler.py
│   └── integration/
│       └── test_api.py
├── docs/
│   ├── architecture.md                # System architecture & component design
│   ├── master_worker_design.md        # Master/worker responsibilities & 9-state lifecycle
│   ├── asyncio_research.md            # Asyncio deep dive, GIL, event loop
│   ├── uvloop_research.md             # uvloop internals, WSL2 & Linux container setup
│   └── system_workflow.md             # End-to-end task workflow & sequences
├── pyproject.toml                     # Build definition & dependencies
├── .env.example                       # Environment configuration template
├── .gitignore
├── README.md
├── PROJECT_CONTEXT.md
└── PROGRESS.md
```

---

## Prerequisites

- **Python 3.11+**
- **pip** (bundled with Python 3.11)
- *(Optional)* **WSL2 Ubuntu 22.04** — required for `uvloop` support; `uvloop` does not support Windows natively. All orchestrator functionality is available on Windows using the standard `asyncio` event loop.

---

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd edge-orchestrator

# Create virtual environment
python -m venv .venv

# Activate (Windows)
.venv\Scripts\activate

# Activate (Linux/macOS/WSL2)
source .venv/bin/activate

# Install (standard — asyncio, Windows compatible)
pip install -e ".[dev]"

# Install with uvloop (Linux/macOS/WSL2 only)
pip install -e ".[uvloop,dev]"
```

---

## Running the API Server

```bash
uvicorn edge_orchestrator.api.server:create_app --factory --host 0.0.0.0 --port 8080
```

The OpenAPI documentation will be available at:
- **Swagger UI:** `http://localhost:8080/docs`
- **ReDoc:** `http://localhost:8080/redoc`

---

## Running Tests

```bash
pytest tests/ -v --cov=src/edge_orchestrator
```

For async tests, `pytest-asyncio` is configured with `asyncio_mode=auto` in `pyproject.toml` — no manual `@pytest.mark.asyncio` decorators are required.

---

## Environment Configuration

Copy the example environment file and populate it with your environment's values:

```bash
cp .env.example .env
```

Edit `.env` to configure:
- `MAX_QUEUE_SIZE` — maximum number of tasks in the queue
- `HEARTBEAT_INTERVAL_SECONDS` — worker heartbeat interval
- `WORKER_TIMEOUT_SECONDS` — default task timeout
- `PROMETHEUS_PORT` — metrics server port

---

## Documentation

| File | Description |
|---|---|
| [`docs/architecture.md`](docs/architecture.md) | Full system architecture: components, data flow, design rationale |
| [`docs/asyncio_research.md`](docs/asyncio_research.md) | Research notes on Python asyncio internals and usage patterns |
| [`docs/uvloop_research.md`](docs/uvloop_research.md) | Research notes on uvloop, benchmarks, and platform compatibility |
| [`docs/master_worker_design.md`](docs/master_worker_design.md) | Detailed design of the master-worker interaction model |
| [`docs/system_workflow.md`](docs/system_workflow.md) | End-to-end task lifecycle from client submission to result delivery |

---

## Development Status

| Phase | Description | Status |
|---|---|---|
| **Phase 1** | Architecture design and project skeleton | ✅ Complete |
| **Phase 2** | Full implementation of all orchestrator components | 🔲 Planned |

Phase 1 deliverables include: all module skeletons, shared Pydantic models, the `MessagingInterface` Protocol contract, `StubMessaging` for local development, the asyncio/uvloop event loop bootstrap, the FastAPI application factory, and all documentation.

---

## ZeroMQ Integration Note

The ZeroMQ transport layer is implemented by **Developer S** as a concrete implementation of the `MessagingInterface` Protocol. The integration contract is defined in [`src/edge_orchestrator/messaging/interface.py`](src/edge_orchestrator/messaging/interface.py) — this file must not be modified without cross-team coordination.

For local development and testing without a ZeroMQ installation, use the in-memory stub:

```python
from edge_orchestrator.messaging.stub import StubMessaging
messaging = StubMessaging()
```

See [`src/edge_orchestrator/messaging/stub.py`](src/edge_orchestrator/messaging/stub.py) for the full stub implementation.

---

## License

This project is licensed under the **MIT License**. See [`LICENSE`](LICENSE) for details.
