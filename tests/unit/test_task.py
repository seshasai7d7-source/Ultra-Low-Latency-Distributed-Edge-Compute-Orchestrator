import pytest
from pydantic import ValidationError

from edge_orchestrator.models.task import Task, TaskStatus


def test_task_creation():
    task = Task(code="print('Hello World')")

    assert task.status == TaskStatus.PENDING
    assert task.timeout_seconds == 30.0
    assert task.max_retries == 0
    assert task.task_id is not None


def test_task_accepts_input_data():
    task = Task(
        code="print(x)",
        input_data={"x": 10},
    )

    assert task.input_data["x"] == 10


def test_task_rejects_empty_code():
    with pytest.raises(ValidationError):
        Task(code="   ")


def test_task_rejects_invalid_timeout():
    with pytest.raises(ValidationError):
        Task(code="print(1)", timeout_seconds=0)


def test_task_rejects_negative_retries():
    with pytest.raises(ValidationError):
        Task(code="print(1)", max_retries=-1)


def test_task_status_can_be_updated():
    task = Task(code="print(1)")
    task.status = TaskStatus.RUNNING

    assert task.status == TaskStatus.RUNNING