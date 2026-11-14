
import multiprocessing
import queue
import traceback
from dataclasses import dataclass
from typing import Any


@dataclass
class ExecutionResult:
    success: bool
    output: Any = None
    error: str | None = None
    execution_time: float = 0.0


def _execute_code(
    code: str,
    input_data: dict[str, Any],
    result_queue: multiprocessing.Queue,
) -> None:
    """Execute code inside a child process."""
    import time

    start_time = time.perf_counter()

    try:
        namespace = {"input_data": input_data}

        # Intentional: execute submitted task code in a child process.
        exec(code, {"__builtins__": __builtins__}, namespace)  # noqa: S102

        result_queue.put({
            "success": True,
            "output": namespace.get("result"),
            "error": None,
            "execution_time": time.perf_counter() - start_time,
        })

    except Exception:  # noqa: BLE001
        # Convert user-code runtime failures into a structured execution result.
        result_queue.put({
            "success": False,
            "output": None,
            "error": traceback.format_exc(),
            "execution_time": time.perf_counter() - start_time,
        })


class WorkerExecutor:
    """Executes Python tasks in isolated child processes."""

    def execute(
        self,
        code: str,
        input_data: dict[str, Any] | None = None,
        timeout_seconds: float = 30.0,
    ) -> ExecutionResult:
        import time

        if timeout_seconds <= 0:
            raise ValueError("Timeout must be greater than zero")

        context = multiprocessing.get_context("spawn")
        result_queue = context.Queue()

        process = context.Process(
            target=_execute_code,
            args=(code, input_data or {}, result_queue),
        )

        start_time = time.perf_counter()
        process.start()
        process.join(timeout_seconds)

        if process.is_alive():
            process.terminate()
            process.join()
            result_queue.close()

            return ExecutionResult(
                success=False,
                error="Task execution timed out",
                execution_time=time.perf_counter() - start_time,
            )

        try:
            result = result_queue.get(timeout=1.0)
        except (queue.Empty, OSError, ValueError):
            result_queue.close()
            return ExecutionResult(
                success=False,
                error="Worker process exited without returning a result",
                execution_time=time.perf_counter() - start_time,
            )

        result_queue.close()

        return ExecutionResult(**result)
