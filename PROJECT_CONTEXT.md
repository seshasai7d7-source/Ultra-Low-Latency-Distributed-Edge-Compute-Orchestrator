# Project Context — Ultra-Low Latency Distributed Edge-Compute Orchestrator

---

## Table of Contents

1. [Project Identity](#1-project-identity)
2. [Team Structure](#2-team-structure)
3. [Architectural Decisions Log](#3-architectural-decisions-log)
4. [Integration Contract](#4-integration-contract)
5. [Development Environments](#5-development-environments)
6. [Conventions](#6-conventions)

---

## 1. Project Identity

| Field | Value |
|---|---|
| **Full Project Name** | Ultra-Low Latency Distributed Edge-Compute Orchestrator |
| **Short Name** | Edge Orchestrator |
| **Repository Status** | Active development |
| **Current Phase** | Phase 1 — Architecture + Skeleton |

### Description

The Edge Orchestrator is a Python-based distributed task execution system targeting ultra-low latency at the edge. It uses a **single-master, multi-worker architecture** in which the master node manages the entire task lifecycle — validation, scheduling, dispatching, and result collection — using an `asyncio`/`uvloop` event loop. Worker nodes receive tasks over a ZeroMQ transport and execute payloads in isolated OS processes. The system is designed for correctness-first development: strict type checking with mypy, Pydantic v2 data models at all boundaries, and a pluggable messaging interface that allows the orchestrator core to be developed and tested independently of the networking layer.

---

## 2. Team Structure

| Developer | Role | Responsibilities | Owns |
|---|---|---|---|
| **Developer V** | System Architect & Orchestrator Engineer | System architecture design; asyncio/uvloop integration; `MasterOrchestrator`, `TaskValidator`, `TaskScheduler`, `TaskDispatcher`, `TaskTracker`; `WorkerManager`, `WorkerNode`, `TaskExecutor`; FastAPI API gateway; security (`ASTValidator`, `SandboxPolicy`); monitoring (Prometheus metrics); shared Pydantic data models (`Task`, `TaskResult`, `WorkerInfo`); `MessagingInterface` Protocol (contract definition only); all documentation in `docs/` | `src/edge_orchestrator/orchestrator/`, `src/edge_orchestrator/worker/`, `src/edge_orchestrator/models/`, `src/edge_orchestrator/api/`, `src/edge_orchestrator/metrics/`, `src/edge_orchestrator/messaging/interface.py`, `src/edge_orchestrator/messaging/stub.py`, `docs/`, `README.md`, `PROJECT_CONTEXT.md`, `PROGRESS.md` |
| **Developer S** | ZeroMQ Networking Engineer | ZeroMQ research and implementation; concrete implementation of `MessagingInterface`; worker communication transport; networking environment and dependency management; ZeroMQ performance testing | `src/edge_orchestrator/messaging/` (concrete ZeroMQ implementation file only) |

---

## 3. Architectural Decisions Log

| # | Decision | Options Considered | Decision Made | Rationale | Date |
|---|---|---|---|---|---|
| 1 | **API Framework** | Flask, Django, FastAPI | **FastAPI** | Async-native (no WSGI blocking); Pydantic built-in for automatic request validation; automatic OpenAPI/Swagger UI generation; explicitly listed as a project specification requirement | Nov 2026 |
| 2 | **Data Models** | Python `dataclasses`, `attrs`, Pydantic | **Pydantic v2** | Runtime type validation at all boundaries; automatic JSON serialisation/deserialisation; native FastAPI integration (request/response models); serves as a shared wire format between master and worker, reducing serialisation bugs | Nov 2026 |
| 3 | **Messaging Boundary** | Direct ZeroMQ import throughout orchestrator | Protocol (interface abstraction) | **Protocol interface** (`MessagingInterface`) | Strict team boundary: Developer V owns the orchestrator and can develop and test it without ZeroMQ installed; Developer S can implement ZeroMQ independently; `StubMessaging` enables full unit and integration tests without network dependencies | Nov 2026 |
| 4 | **Package Layout** | Flat layout (modules at repo root) | src-layout (PEP 517) | **src-layout** | Prevents accidental imports from the project root directory during development; standard layout for installable packages distributed via pip; enforced by `pyproject.toml` with `package-dir = {"" = "src"}` | Nov 2026 |
| 5 | **Event Loop** | asyncio only (always), uvloop mandatory | asyncio with uvloop as optional extra | **asyncio with uvloop optional** | `uvloop` does not support Windows; Developer V uses Windows for daily development; marking uvloop as an optional install extra (`[uvloop]`) retains Windows compatibility while enabling uvloop in WSL2 and production Linux environments | Nov 2026 |
| 6 | **Task State Representation** | Plain string constants, `enum.Enum` | **`enum.Enum`** | Type safety prevents typos in state names; IDE autocompletion for all state references; `TaskTracker` can enforce valid transitions using the enum's closed value set; invalid state values caught at development time rather than runtime | Nov 2026 |
| 7 | **CPU Workload Execution** | Execute task payload in-process (asyncio coroutine) | Execute in isolated OS process | **Isolated OS process** | The asyncio event loop is single-threaded and must never be blocked; CPU-bound task payloads would stall all concurrent orchestrator operations if run in-process; isolated worker processes ensure the event loop remains responsive regardless of payload complexity | Nov 2026 |
| 8 | **Task Lifecycle States** | 5 states, 7 states | **9-state lifecycle** | Explicit representation of all stages: `PENDING`, `VALIDATED`, `QUEUED`, `DISPATCHED`, `RUNNING`, `COMPLETED`, `FAILED`, `TIMED_OUT`, `CANCELLED`. Avoids losing audit trail for cancellations vs timeouts | Nov 2026 |
| 9 | **Security Sandboxing** | AST checks only, Restricted builtins | **Two-stage security (AST + OS isolation)** | AST validation is purely static pre-filtering on the master. Real runtime isolation is strictly enforced on workers via separate processes, POSIX rlimits, and container boundaries | Nov 2026 |
| 10 | **Untrusted Worker Execution** | ProcessPoolExecutor for all, Dedicated Subprocess | **Dedicated Child Processes** | Untrusted code payloads must execute in dedicated, ephemeral child processes. `ProcessPoolExecutor` reuses processes across tasks, causing state and memory bleed, and is NOT a security sandbox; it is restricted to trusted internal callables | Nov 2026 |
| 11 | **Task Store & Eviction** | Direct dictionary, Persistent DB | **TaskStore Protocol + InMemoryTaskStore** | Initial prototype uses `InMemoryTaskStore` (10,000 capacity). Enforces safe eviction: active tasks (`PENDING`..`RUNNING`) are NEVER evicted; only completed/terminal tasks are purged in FIFO order; raises error if all 10,000 are active | Nov 2026 |
| 12 | **Task Acknowledgement Semantics** | Single Ack triggers RUNNING | **Two-Stage Confirmation (Receipt Ack + Started)** | Worker receipt ack (`TaskAcknowledgement`) confirms delivery and queuing; task remains in `DISPATCHED`. Transition to `RUNNING` occurs ONLY when worker emits `TaskStarted` notification | Nov 2026 |

---

## 4. Integration Contract

### MessagingInterface Protocol

`MessagingInterface` is a Python `typing.Protocol` defined in `src/edge_orchestrator/messaging/interface.py`. It specifies the **exact method signatures** that any concrete messaging implementation must satisfy. Developer S's ZeroMQ implementation must conform to this protocol without modification.

```python
class MessagingInterface(Protocol):
    # Connection Lifecycle
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def connect_worker(self, worker_id: str, address: str) -> None: ...
    async def disconnect_worker(self, worker_id: str) -> None: ...

    # Worker Registration Handshake
    def listen_for_registrations(self) -> AsyncIterator[WorkerInfo]: ...
    async def send_registration_ack(self, worker_id: str, accepted: bool, reason: str = "") -> None: ...

    # Task Operations & Two-Stage Confirmation
    async def send_task(self, worker_id: str, task: Task) -> None: ...
    async def await_task_ack(self, worker_id: str, task_id: str, timeout_seconds: float = 5.0) -> Optional[TaskAcknowledgement]: ...
    async def await_task_started(self, worker_id: str, task_id: str, timeout_seconds: float = 10.0) -> Optional[TaskStarted]: ...
    async def send_cancellation(self, worker_id: str, task_id: str) -> None: ...

    # Result Reception & Heartbeats
    async def receive_result(self, timeout_seconds: float = 30.0) -> TaskResult: ...
    def listen_for_heartbeats(self) -> AsyncIterator[HeartbeatMessage]: ...
```

#### Direction and Semantics of Acknowledgements
- **Registration Acknowledgement (`send_registration_ack`)**: Master -> Worker. Confirms if the worker has been accepted into the active orchestrator pool.
- **Task Receipt Acknowledgement (`await_task_ack`)**: Worker -> Master. Confirms that the worker node received and queued the task. **Task remains in DISPATCHED**.
- **Task Started Event (`await_task_started`)**: Worker -> Master. Confirms worker spawned dedicated execution child process. **Triggers transition DISPATCHED -> RUNNING**.

#### Clean Separation of Concerns
- Developer S owns socket handling, frame serialization, ZeroMQ topologies (e.g. ROUTER/DEALER or PUSH/PULL), and socket buffer lifecycles.
- Developer V owns timeout detection, worker failure decisions, retry counters, task scheduling, and state transitions. Transport never makes business or state decisions.

### Files Developer S Must NOT Modify

The following files are entirely owned by Developer V. Developer S must not modify them without explicit cross-team coordination:

| File | Reason |
|---|---|
| `src/edge_orchestrator/messaging/interface.py` | Defines the integration contract both developers depend on |
| `src/edge_orchestrator/messaging/stub.py` | Test infrastructure used by Developer V's integration tests |
| `src/edge_orchestrator/models/task.py` | Shared wire format (`Task`, `TaskStatus`, `TaskAcknowledgement`) |
| `src/edge_orchestrator/models/result.py` | Shared wire format (`TaskResult`) |
| `src/edge_orchestrator/models/worker.py` | Shared wire format (`WorkerInfo`, `HeartbeatMessage`) |
| All other `src/edge_orchestrator/` files | Orchestrator core, scheduler, tracker, API; not part of Developer S's scope |
| All `docs/` files | Technical documentation; maintained by Developer V |

### Shared Models Developer S Depends On

Developer S's ZeroMQ implementation reads and writes the following models as the wire format:

| Model | File | Used For |
|---|---|---|
| `Task` | `models/task.py` | Serialised by master and sent to worker |
| `TaskAcknowledgement` | `models/task.py` | Serialised by worker confirming task receipt |
| `TaskResult` | `models/result.py` | Serialised by worker and sent to master |
| `WorkerInfo` | `models/worker.py` | Worker registration frame |
| `HeartbeatMessage` | `models/worker.py` | Worker periodic liveness frame |

> **Important:** Any change to field names, types, or validators in these model files changes the wire format and may break the ZeroMQ transport layer. Changes to shared models **require cross-team coordination** before merging.

---

## 5. Development Environments

| Environment | OS | Python | uvloop | Use Case |
|---|---|---|---|---|
| **Windows 11** | Win32 (native) | 3.11 | ❌ Not supported | Developer V daily development; standard asyncio event loop |
| **WSL2 Ubuntu 22.04** | Linux (kernel 5.15+) | 3.11 | ✅ Supported | Integration tests; uvloop performance benchmarks; Linux-specific sandbox policy testing |
| **Docker (`python:3.11-slim`)** | Linux | 3.11 | ✅ Supported | Staging and production deployments; CI pipeline execution |

### Environment-specific notes

- **Windows:** Install with `pip install -e ".[dev]"`. The uvloop extra is not available; the standard asyncio event loop is used automatically.
- **WSL2:** Install with `pip install -e ".[uvloop,dev]"`. The orchestrator detects uvloop availability at startup and activates it automatically.
- **Docker:** Use the provided `Dockerfile` (Phase 2 deliverable). The base image is `python:3.11-slim`; uvloop and ZeroMQ native libraries are installed in the image build.

---

## 6. Conventions

### Language and Runtime

- **Python version:** 3.11+ (required; uses `tomllib`, `ExceptionGroup`, and improved typing features)
- **Type hints:** Strict — all functions and methods must have full type annotations; `mypy --strict` must pass with zero errors

### Code Style

- **Linter/formatter:** `ruff` with line length 100
- **Import order:** stdlib → third-party → local (enforced by `ruff` import sorting rules)
- **String quotes:** Double quotes (enforced by ruff)

### Testing

- **Framework:** `pytest` with `pytest-asyncio`
- **Async mode:** `asyncio_mode = auto` in `pyproject.toml` — all `async def test_*` functions are automatically treated as async tests
- **Coverage:** Minimum 80% line coverage enforced in CI

### Documentation

- **Docstring style:** NumPy / Google hybrid — all public functions must include `Parameters`, `Returns`, and `Raises` sections
- **Inline comments:** Explain *why*, not *what*

### Async Rules

> **Golden rule: never block the event loop.**

- Never call synchronous file I/O (`open`, `read`, `write`) in a coroutine — use `aiofiles` or offload to a thread pool executor
- Never call `time.sleep()` in a coroutine — use `await asyncio.sleep()`
- Never perform CPU-intensive work in a coroutine — offload to a worker process via the `TaskExecutor`
- All external I/O (ZeroMQ send/receive, database, metrics push) must be `await`-able

### Version Control

- Branch naming: `feature/<short-description>`, `fix/<short-description>`, `docs/<short-description>`
- Commit messages: imperative mood, 72-character subject line, blank line before body
- Pull requests require passing `mypy --strict`, `ruff check`, and `pytest` before merge
