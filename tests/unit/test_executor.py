
import pytest

from edge_orchestrator.worker.executor import WorkerExecutor


@pytest.fixture
def executor():
    return WorkerExecutor()


def test_execute_simple_code(executor):
    result = executor.execute("result = 10 + 20")

    assert result.success is True
    assert result.output == 30
    assert result.error is None


def test_execute_code_with_input(executor):
    result = executor.execute(
        "result = input_data['a'] + input_data['b']",
        {"a": 10, "b": 5},
    )

    assert result.success is True
    assert result.output == 15


def test_execute_function(executor):
    code = """
def multiply(a, b):
    return a * b

result = multiply(4, 5)
"""

    result = executor.execute(code)

    assert result.success is True
    assert result.output == 20


def test_execution_error_is_captured(executor):
    result = executor.execute("result = 10 / 0")

    assert result.success is False
    assert "ZeroDivisionError" in result.error


def test_execution_timeout(executor):
    result = executor.execute(
        "while True: pass",
        timeout_seconds=1,
    )

    assert result.success is False
    assert result.error == "Task execution timed out"


def test_invalid_timeout_is_rejected(executor):
    with pytest.raises(ValueError):
        executor.execute("result = 1", timeout_seconds=0)


def test_execution_time_is_recorded(executor):
    result = executor.execute("result = sum(range(100))")

    assert result.success is True
    assert result.execution_time >= 0
