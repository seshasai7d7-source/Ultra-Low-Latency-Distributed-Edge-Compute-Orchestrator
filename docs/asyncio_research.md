# Asyncio Research — Python Asynchronous Programming for the Edge Orchestrator

| Field   | Value             |
|---------|-------------------|
| Version | 0.1.0             |
| Author  | Developer V       |
| Date    | November 2026     |

---

## Table of Contents

1. [Overview of Python Asynchronous Programming](#1-overview-of-python-asynchronous-programming)
2. [The Event Loop](#2-the-event-loop)
3. [Coroutines](#3-coroutines)
4. [async and await](#4-async-and-await)
5. [asyncio Tasks](#5-asyncio-tasks)
6. [Concurrent Task Execution](#6-concurrent-task-execution)
7. [Asynchronous Task Scheduling in the Orchestrator](#7-asynchronous-task-scheduling-in-the-orchestrator)
8. [asyncio and CPU-Intensive Workloads — Critical Limitation](#8-asyncio-and-cpu-intensive-workloads--critical-limitation)
9. [How asyncio Improves Multi-Worker Handling](#9-how-asyncio-improves-multi-worker-handling)
10. [asyncio Patterns Used in This Project](#10-asyncio-patterns-used-in-this-project)

---

## 1. Overview of Python Asynchronous Programming

### Synchronous Programming and Its Limits in I/O-Heavy Systems

In synchronous programming, statements execute one after another in a strict sequential order. When a function initiates an I/O operation — opening a socket, sending a network request, waiting for a reply from a remote worker node — the calling thread **blocks**. The CPU sits idle, waiting for the kernel to signal that data is available. No other work can proceed in that thread during this wait.

For a system like the edge orchestrator, which must simultaneously maintain connections to dozens or hundreds of geographically distributed worker nodes, this model is catastrophically inefficient. If dispatching a task to a worker takes 50 ms of network round-trip time and we handle workers one at a time, dispatching to 100 workers takes a minimum of 5 seconds — an unacceptable latency floor for a real-time orchestration system.

### What Asynchronous Programming Solves

Asynchronous programming breaks the one-thread-one-operation constraint. Rather than blocking on a slow I/O call, an asynchronous function **suspends itself** and allows the runtime to execute other work while the kernel waits for I/O to complete. When the I/O finishes, the suspended function is **resumed** from where it left off.

This model allows a single thread to interleave hundreds of concurrent I/O operations with virtually zero OS-level overhead, because the thread never truly blocks — it always has something productive to do.

### Python's Threading Model: The GIL

Python's reference implementation (CPython) includes the **Global Interpreter Lock (GIL)**: a mutex that ensures only one native OS thread executes Python bytecode at a time. This design simplifies CPython's internal memory management but has a critical consequence: even if you create 100 Python threads, only one of them runs Python code at any instant.

For CPU-bound work, threads provide no parallelism under the GIL. For I/O-bound work, threads *can* interleave because the GIL is released during blocking system calls (the kernel takes over and the Python bytecode is not running). However, threading still carries substantial costs:

- Each OS thread consumes a fixed memory stack (typically 1–8 MB on Linux).
- Context switching between OS threads is a kernel operation with non-trivial overhead.
- Shared mutable state requires careful locking (`threading.Lock`, etc.) to avoid races.
- Python's thread scheduling is non-deterministic; under high contention the GIL can cause "thundering herd" behavior.

For an orchestrator maintaining hundreds of persistent worker connections, 100+ threads is expensive and hard to reason about. The asyncio model — one thread, event-driven concurrency — is architecturally superior for this workload.

---

## 2. The Event Loop

### What an Event Loop Is

The event loop is the central scheduler of an asyncio program. It is a loop that repeatedly:

1. Checks whether any I/O descriptors (sockets, pipes, etc.) are ready for reading or writing.
2. Fires any callbacks or resumes any coroutines associated with ready descriptors.
3. Runs any coroutines that have been scheduled via `asyncio.create_task()` and are not currently suspended.
4. Sleeps (via the OS) until the next I/O event or timer fires, consuming zero CPU.

The loop never does useful computation itself — it only decides *which* coroutine gets to run next, based on which I/O events have occurred.

### OS-Level I/O Multiplexing

Under the hood, the event loop delegates I/O monitoring to the operating system via a **multiplexing** system call:

- **Linux**: `epoll` — scales to thousands of file descriptors with O(1) event notification.
- **macOS / BSD**: `kqueue` — equivalent to epoll on BSD-derived kernels.
- **Windows**: `select` or `IOCP` (I/O Completion Ports) — Python's asyncio uses `ProactorEventLoop` on Windows.

Python's standard library abstracts these via the `selectors` module. The event loop calls `selectors.select()`, which blocks in the OS kernel until at least one monitored descriptor is ready. No Python bytecode executes during this kernel-level sleep — the CPU is genuinely idle, not spinning.

### The Single-Thread Guarantee

Because the event loop runs in a single OS thread, **at most one coroutine is ever executing Python code at any instant**. This is a profound safety property: shared Python objects — dictionaries, lists, custom data structures — are **never accessed concurrently** by two coroutines simultaneously. As long as a coroutine does not voluntarily yield (i.e., does not reach an `await` expression), it has exclusive access to all shared state.

This eliminates entire classes of race conditions that plague multi-threaded code. Locks are still needed in some cases (see Section 10), but the invariant is far simpler to reason about.

### asyncio.get_event_loop() and asyncio.run()

```python
import asyncio

# Modern entry point (Python 3.7+): creates a new event loop, runs the
# coroutine to completion, closes the loop, and returns the result.
asyncio.run(main())

# Legacy / advanced: get the running loop from inside a coroutine or callback.
loop = asyncio.get_event_loop()
```

`asyncio.run()` is the recommended entry point for all new code. It:
- Creates a fresh event loop.
- Runs the given coroutine as the **main task**.
- Cancels all remaining tasks on completion.
- Closes the loop and releases resources.

`asyncio.get_running_loop()` (Python 3.7+) safely retrieves the loop that is currently executing — preferred over `get_event_loop()` inside async code.

### Relevance to the Orchestrator

In the edge orchestrator, the event loop is the master node's **only OS thread**. All worker connections, all task dispatches, all heartbeat checks, and all result collections are I/O operations that run concurrently within this single loop. The loop's scheduler decides, millisecond by millisecond, which worker's response to process next. No threads, no locks on worker connections, no context-switch overhead.

---

## 3. Coroutines

### What a Coroutine Is

A **coroutine** is a function that can be **suspended mid-execution** and **resumed later** without losing its local state. It is a generalization of a subroutine: where a normal function runs from start to end and returns, a coroutine can pause at designated suspension points and yield control back to its caller (the event loop).

Coroutines in Python 3.5+ are defined using the `async def` syntax and suspended using the `await` expression.

### async def Syntax

```python
async def fetch_worker_result(worker_id: str, timeout: float) -> dict:
    """Coroutine: suspend until a worker's result arrives or timeout fires."""
    # ... actual implementation uses await internally
    result = await asyncio.wait_for(
        _read_from_worker_socket(worker_id),
        timeout=timeout
    )
    return result
```

Defining a function with `async def` does **not** execute its body. It creates a **coroutine function**. Calling the function (e.g., `fetch_worker_result("w1", 5.0)`) returns a **coroutine object** — a paused execution context that has not yet started.

### How Coroutines Differ from Regular Functions and Generators

| Property | Regular Function | Generator | Coroutine |
|---|---|---|---|
| Defined with | `def` | `def` + `yield` | `async def` |
| Returns | Value immediately | Generator iterator | Coroutine object |
| Can suspend? | No | Yes (at `yield`) | Yes (at `await`) |
| Resumed by | n/a | Caller calls `next()` | Event loop |
| Can receive values? | n/a | Via `send()` | Via `await` result |

Coroutines are built on top of Python's generator protocol (`__iter__`, `__next__`, `send`, `throw`, `close`) but are a distinct type. They cannot be driven with `next()` — they must be awaited or scheduled as a Task.

### Coroutine Object vs. Coroutine Call

```python
# This creates a coroutine OBJECT — nothing executes yet.
coro = fetch_worker_result("worker-42", timeout=5.0)

# This SCHEDULES and RUNS the coroutine to completion.
result = await coro

# Or, equivalently, in one expression:
result = await fetch_worker_result("worker-42", timeout=5.0)
```

The distinction is critical. A coroutine object that is never awaited or scheduled simply does nothing and is eventually garbage-collected. Python 3.6+ emits a `RuntimeWarning: coroutine '...' was never awaited` to alert the developer.

### Code Example: Simulating a Worker Response

```python
import asyncio
import random

async def simulate_worker_response(worker_id: str) -> dict:
    """
    Simulates the latency of sending a task to a remote worker and
    waiting for its acknowledgement.
    """
    # Simulate network round-trip: 10–200 ms
    latency_ms = random.uniform(10, 200)
    await asyncio.sleep(latency_ms / 1000.0)  # suspension point

    return {
        "worker_id": worker_id,
        "status": "ACK",
        "latency_ms": round(latency_ms, 2),
    }

async def main():
    response = await simulate_worker_response("edge-worker-07")
    print(f"Worker responded: {response}")

asyncio.run(main())
```

At the `await asyncio.sleep(...)` line, the coroutine suspends and returns control to the event loop. The event loop can run other coroutines during this period. When the sleep timer fires, the event loop resumes `simulate_worker_response` on the next line.

---

## 4. async and await

### What await Does

The `await` expression is the coroutine's suspension mechanism. When a coroutine executes `await expr`:

1. `expr` is evaluated — it must produce an **awaitable** object.
2. If the awaitable is not yet complete, the current coroutine **suspends**: its stack frame is preserved and control is returned to the event loop.
3. The event loop continues processing other ready coroutines and I/O events.
4. When the awaitable completes (e.g., data arrives on a socket), the event loop **resumes** the suspended coroutine, passing the result of the awaitable as the value of the `await` expression.

The key point: `await` does **not** block the OS thread. The thread remains in the event loop's control, free to do other work.

### What Can Be Awaited

An object is **awaitable** if it implements the `__await__()` dunder method (which must return an iterator). The following types are natively awaitable:

- **Coroutine objects** — the result of calling an `async def` function.
- **`asyncio.Task` objects** — a coroutine wrapped with `asyncio.create_task()`.
- **`asyncio.Future` objects** — a low-level promise/placeholder for a result.
- **Objects implementing `__await__`** — custom awaitables (e.g., `asyncio.Event`, `asyncio.Lock`).

Standard Python objects (integers, strings, lists, etc.) are **not** awaitable. Attempting `await 42` raises `TypeError: object int can't be used in 'await' expression`.

### Why await Cannot Be Used Outside async def

The `await` expression compiles to bytecode instructions (`GET_AWAITABLE`, `YIELD_FROM`) that only make sense inside the coroutine protocol. The Python compiler rejects `await` outside an `async def` function at **parse time** with a `SyntaxError`.

```python
# SyntaxError: 'await' outside function
result = await some_coroutine()
```

### Common Mistake: Calling a Coroutine Without Awaiting

```python
async def dispatch_task(task_id: str):
    await _send_to_worker(task_id)

async def main():
    # BUG: dispatch_task returns a coroutine object; nothing actually executes.
    dispatch_task("task-001")   # RuntimeWarning: coroutine was never awaited

    # CORRECT: await the coroutine.
    await dispatch_task("task-001")

    # ALSO CORRECT: schedule it as a concurrent Task.
    asyncio.create_task(dispatch_task("task-001"))
```

Python 3.6+ issues a `RuntimeWarning` for unawaited coroutines, but it is a warning, not an exception — the bug is silent in production unless warnings are promoted to errors (recommended in test suites via `pytest -W error::RuntimeWarning`).

---

## 5. asyncio Tasks

### asyncio.Task vs. Coroutine

A raw coroutine is **not scheduled** until it is either awaited or wrapped in a Task. When you `await` a coroutine directly, the current coroutine suspends until that coroutine finishes — it is sequential, not concurrent.

An **`asyncio.Task`** wraps a coroutine and schedules it to run **concurrently** with the current coroutine. The event loop will interleave both tasks at their `await` points.

```python
# Sequential: main waits for coroutine_a to finish before starting coroutine_b
await coroutine_a()
await coroutine_b()

# Concurrent: both are scheduled immediately; event loop interleaves them
task_a = asyncio.create_task(coroutine_a())
task_b = asyncio.create_task(coroutine_b())
await task_a
await task_b
```

### asyncio.create_task()

```python
task = asyncio.create_task(coro, name="optional-task-name")
```

`create_task()`:
- Wraps `coro` in a `Task` object.
- Schedules the Task to begin on the next iteration of the event loop.
- Returns the Task immediately (the coroutine has not started yet).
- The Task runs in the background — the caller does not need to await it immediately.

**When to use it**: whenever a coroutine should run concurrently rather than sequentially. Background loops (dispatch loop, heartbeat monitor, result collector) are always created with `create_task()`.

### Task Lifecycle

```
create_task()
     │
     ▼
 PENDING ──► RUNNING ──► DONE
                │
                ▼
           CANCELLED
```

- **PENDING**: Task created, not yet started by the event loop.
- **RUNNING**: Currently executing (at an `await` point it briefly yields to the loop, but conceptually it is still running).
- **DONE**: Coroutine completed, raised an exception, or was cancelled.
- **CANCELLED**: `task.cancel()` was called and a `CancelledError` was delivered to the coroutine.

Inspect a task's state with `task.done()`, `task.cancelled()`, `task.result()`, `task.exception()`.

### asyncio.gather()

```python
results = await asyncio.gather(coro1(), coro2(), coro3())
# results is a list: [result_of_coro1, result_of_coro2, result_of_coro3]
```

`asyncio.gather()` schedules all passed coroutines (or Tasks/Futures) concurrently and waits for **all** of them to complete. Results are returned in the **same order as the inputs**, regardless of completion order.

By default, if any coroutine raises an exception, `gather()` propagates it immediately and cancels remaining tasks. Pass `return_exceptions=True` to collect exceptions as results instead.

### asyncio.wait()

```python
done, pending = await asyncio.wait(
    {task1, task2, task3},
    return_when=asyncio.FIRST_COMPLETED  # or ALL_COMPLETED, FIRST_EXCEPTION
)
```

`asyncio.wait()` offers more granular control:
- `FIRST_COMPLETED`: return as soon as any task finishes (useful for racing multiple workers).
- `FIRST_EXCEPTION`: return as soon as any task raises an exception.
- `ALL_COMPLETED`: equivalent to `gather()` behavior, but returns sets rather than ordered results.

Returns two sets: `done` (finished tasks) and `pending` (still running tasks). Pending tasks are **not cancelled** — the caller decides what to do with them.

### Code Example: Dispatching to 5 Workers Concurrently

```python
import asyncio
import random

async def dispatch_to_worker(worker_id: str, task_payload: dict) -> dict:
    """Simulate dispatching a task to a remote edge worker."""
    latency = random.uniform(0.05, 0.3)   # 50–300 ms
    await asyncio.sleep(latency)           # network round-trip
    return {
        "worker_id": worker_id,
        "task_id": task_payload["id"],
        "status": "DISPATCHED",
        "latency_s": round(latency, 3),
    }

async def dispatch_batch(task_payload: dict, worker_ids: list[str]) -> list[dict]:
    """Dispatch the same task to multiple workers and collect all responses."""
    coroutines = [dispatch_to_worker(wid, task_payload) for wid in worker_ids]
    results = await asyncio.gather(*coroutines, return_exceptions=True)
    return results

async def main():
    workers = [f"edge-worker-{i:02d}" for i in range(1, 6)]
    task = {"id": "task-9981", "type": "inference", "priority": 1}

    print(f"Dispatching to {len(workers)} workers concurrently...")
    results = await dispatch_batch(task, workers)

    for r in results:
        if isinstance(r, Exception):
            print(f"  ERROR: {r}")
        else:
            print(f"  Worker {r['worker_id']}: {r['status']} in {r['latency_s']}s")

asyncio.run(main())
```

All five dispatches run concurrently. Total elapsed time is dominated by the **slowest** worker's latency, not the sum.

---

## 6. Concurrent Task Execution

### Cooperative Multitasking

asyncio uses **cooperative multitasking**: a coroutine runs until it explicitly yields by reaching an `await` expression. The event loop does not preempt coroutines mid-execution (unlike OS thread scheduling, which is preemptive). This means:

- Coroutines decide when to yield control.
- Shared state is safe between `await` points (no concurrent mutation is possible).
- Developers must write coroutines that yield frequently enough for other tasks to get CPU time.

This is the opposite of OS threads, where the kernel scheduler can interrupt a thread at any instruction boundary.

### Why This Is Safe for I/O-Bound Workloads

For the orchestrator's workload — sending messages to workers and waiting for replies — the dominant time is **waiting for the network**. A typical task dispatch coroutine spends 95%+ of its time suspended at an `await` point, waiting for a socket to become readable. During that wait, the event loop runs other coroutines that are ready. The CPU is never idle; it is always making progress on some ready task.

This model is a natural fit for I/O-bound concurrency. The key insight: **waiting is not wasting** — coroutines suspend rather than block, leaving the CPU free for productive work.

### Managing N Worker Communications Concurrently

A single event loop can maintain persistent TCP connections to N workers and multiplex I/O across all of them via `epoll` (Linux). When any of those N sockets has data ready, `epoll` returns the specific file descriptor, the event loop looks up the coroutine waiting on that descriptor, and resumes it. The other N-1 coroutines remain suspended, consuming no CPU.

In practice, the event loop handles **tens of thousands** of concurrent socket connections on modern hardware — far more than the worker counts in the orchestrator.

### Starvation: What Happens When a Coroutine Never Awaits

If a coroutine performs a CPU-intensive computation without reaching any `await` expression:

```python
async def bad_coroutine():
    # This tight loop runs for 10 seconds without ever yielding.
    result = sum(i * i for i in range(100_000_000))
    return result
```

The event loop **cannot** interrupt it. For the entire 10 seconds:
- No other coroutine runs.
- No heartbeats are sent to workers.
- No task results are collected.
- The orchestrator appears frozen.

This is called **event loop starvation** and is one of the most common bugs in asyncio applications. The mitigation is covered in Section 8.

---

## 7. Asynchronous Task Scheduling in the Orchestrator

The orchestrator master node runs three concurrent background loops inside the same event loop:

### asyncio.PriorityQueue for Task Scheduling

`asyncio.PriorityQueue` is an async-safe, coroutine-friendly priority queue. Items are sorted by their priority value (lower value = higher priority). The orchestrator's `task_queue.py` uses it as the central task buffer:

```python
# Put a task into the queue (from an HTTP handler coroutine)
await task_queue.put((priority, task))

# Get the highest-priority task (from the dispatch loop coroutine)
priority, task = await task_queue.get()
```

Both `put()` and `get()` are coroutines that suspend if the queue is full or empty, respectively. This means neither the HTTP handler nor the dispatch loop ever blocks the event loop — they simply suspend until the queue state changes.

### Dispatch Loop

The dispatch loop is a long-running coroutine wrapped in a Task:

```python
async def dispatch_loop(task_queue, worker_manager, dispatcher):
    while True:
        priority, task = await task_queue.get()       # suspend until task available
        worker = await worker_manager.get_free_worker()  # suspend until worker free
        asyncio.create_task(dispatcher.send(worker, task))  # non-blocking dispatch
        task_queue.task_done()
```

It runs as `asyncio.create_task(dispatch_loop(...))` from the orchestrator startup. It yields at each `await`, allowing other loops to run between task dispatches.

### Heartbeat Monitor

```python
async def heartbeat_loop(worker_manager, interval_s: float = 5.0):
    while True:
        await asyncio.sleep(interval_s)              # yield for 5 seconds
        await worker_manager.send_heartbeats()       # ping all workers
```

Runs concurrently with the dispatch loop. The `await asyncio.sleep(5.0)` ensures it yields frequently, never starving other tasks.

### Result Collector

```python
async def result_collection_loop(result_queue, task_tracker):
    while True:
        result = await result_queue.get()            # suspend until result arrives
        await task_tracker.update(result)
```

All three loops — dispatch, heartbeat, result collection — run as independent Tasks in the same event loop. They interleave at their `await` points with zero OS context switching overhead.

---

## 8. asyncio and CPU-Intensive Workloads — Critical Limitation

### The Core Problem

A CPU-bound computation inside the event loop **blocks all other coroutines** for its entire duration. There is no preemption, no work-stealing, no thread switch. The event loop is a single thread — if that thread is busy computing, it cannot process I/O events, it cannot run heartbeat tasks, it cannot collect results.

**Example of the problem**:

```python
async def preprocess_model_input(data: bytes) -> np.ndarray:
    # This runs entirely in the event loop thread.
    # During the 500ms this takes, ZERO other coroutines run.
    return heavy_numpy_transform(data)   # 500 ms of pure CPU work
```

For a latency-sensitive orchestrator, a 500 ms stall is catastrophic: worker heartbeat timeouts may fire, task results may queue up unprocessed, and the HTTP API becomes unresponsive.

### Why This Matters for the Orchestrator

The orchestrator's master node is the single point of coordination for all workers. If it stalls, all workers are effectively unreachable. Any CPU-intensive operation in the master's event loop directly degrades the latency and reliability of the entire distributed system.

### The Solution: Workers Are Separate OS Processes

The architectural answer is: **the event loop does not perform computation**. All CPU-intensive work (model inference, data preprocessing, job execution) is performed by **separate worker node processes** — independent OS processes with their own Python interpreters, their own GILs, and their own CPUs.

The master event loop only performs:
- I/O: reading and writing to worker sockets.
- Coordination: priority queue management, task state tracking.
- Protocol: serializing/deserializing messages (kept intentionally lightweight).

This keeps the event loop's CPU usage consistently low and its latency consistently bounded.

### asyncio.run_in_executor (Brief Note)

For cases where a small amount of CPU work cannot be avoided in the master (e.g., request deserialization with a heavy codec), `asyncio.run_in_executor` offloads the work to a thread pool without blocking the event loop:

```python
loop = asyncio.get_running_loop()
result = await loop.run_in_executor(None, cpu_bound_function, arg1, arg2)
```

`None` uses the default `ThreadPoolExecutor`. For truly CPU-bound work, `ProcessPoolExecutor` is preferable (bypasses the GIL). However, in this project's architecture, significant CPU work always belongs in a worker process, not in the master.

---

## 9. How asyncio Improves Multi-Worker Handling

### The Threading Alternative

Without asyncio, maintaining connections to 100 worker nodes would conventionally require one thread per worker (or one thread per active operation). The costs:

| Resource | Per-Thread Cost | 100 Threads Total |
|---|---|---|
| Stack memory | ~1–8 MB (OS default) | 100–800 MB |
| OS thread object | ~8 KB kernel struct | ~800 KB kernel memory |
| Context switch (Linux) | ~1–10 µs | Potentially thousands/sec |
| Lock contention (GIL) | Varies | Serializes all Python ops |
| Scheduler overhead | O(n) with thread count | Increases with scale |

At 100 threads, context switch overhead is measurable. At 1,000 threads, it becomes a dominant cost. Memory consumption alone may exceed practical limits.

### The asyncio Alternative

With asyncio:

| Resource | Per-Coroutine Cost | 100 Coroutines Total |
|---|---|---|
| Stack memory | Python frame object (~few KB) | A few hundred KB |
| OS threads | 1 (the event loop thread) | 1 |
| Context switch | 0 (cooperative yield, no kernel call) | 0 kernel transitions |
| I/O multiplexing | epoll: O(1) per event | Scales to 100,000+ fds |

One event loop thread handles all 100 worker connections. Switching between coroutines is a pure Python operation (resuming an iterator), not a kernel context switch. The savings in both memory and CPU are substantial.

### Result Handling in Arrival Order

The event loop processes worker results as they arrive. If worker-07 responds before worker-02 (because its network path is faster), the result from worker-07 is processed first — there is no polling, no busy waiting, no sequential check of each worker. `epoll` delivers the notification the moment the kernel receives data on any socket. This arrival-order processing is a natural property of the event-driven model.

---

## 10. asyncio Patterns Used in This Project

| Pattern | Where Used | Why |
|---|---|---|
| `asyncio.PriorityQueue` | `task_queue.py` | Async-safe task buffer with priority ordering; `put()`/`get()` suspend instead of block |
| `asyncio.Lock` | `task_tracker.py`, `worker_manager.py` | Protects shared mutable state (task registry, worker pool) across the single await-point-separated concurrent accesses |
| `asyncio.create_task` | `orchestrator.py` | Launches dispatch loop, heartbeat monitor, and result collector as concurrent background Tasks at orchestrator startup |
| `asyncio.gather` | `dispatcher.py` | Dispatches a task to multiple candidate workers simultaneously and collects all acknowledgements before selecting the fastest |
| `asyncio.wait_for` | `dispatcher.py` | Enforces a per-task dispatch timeout; raises `asyncio.TimeoutError` if a worker does not respond within the SLA window |
| Async generators (`async def` + `yield`) | `messaging/interface.py` | Streams worker registration events and heartbeat messages without buffering the entire sequence in memory |

---

*Document version 0.1.0 — Subject to revision as implementation matures.*
