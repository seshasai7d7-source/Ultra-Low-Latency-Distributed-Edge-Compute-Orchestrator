import pytest

from edge_orchestrator.core.lifecycle import (
    InvalidTaskTransition,
    TaskLifecycleManager,
)
from edge_orchestrator.models.task import Task, TaskStatus


def test_valid_task_transition():
    task = Task(code="print(1)")
    manager = TaskLifecycleManager()

    manager.transition(task, TaskStatus.VALIDATED)

    assert task.status == TaskStatus.VALIDATED


def test_complete_task_lifecycle():
    task = Task(code="print(1)")
    manager = TaskLifecycleManager()

    transitions = [
        TaskStatus.VALIDATED,
        TaskStatus.QUEUED,
        TaskStatus.DISPATCHED,
        TaskStatus.RUNNING,
        TaskStatus.COMPLETED,
    ]

    for status in transitions:
        manager.transition(task, status)

    assert task.status == TaskStatus.COMPLETED


def test_invalid_transition_is_rejected():
    task = Task(code="print(1)")
    manager = TaskLifecycleManager()

    with pytest.raises(InvalidTaskTransition):
        manager.transition(task, TaskStatus.RUNNING)

    assert task.status == TaskStatus.PENDING


def test_terminal_task_cannot_transition():
    task = Task(code="print(1)")
    manager = TaskLifecycleManager()

    manager.transition(task, TaskStatus.VALIDATED)
    manager.transition(task, TaskStatus.QUEUED)
    manager.transition(task, TaskStatus.DISPATCHED)
    manager.transition(task, TaskStatus.RUNNING)
    manager.transition(task, TaskStatus.COMPLETED)

    with pytest.raises(InvalidTaskTransition):
        manager.transition(task, TaskStatus.RUNNING)


def test_running_task_can_be_cancelled():
    task = Task(code="print(1)")
    manager = TaskLifecycleManager()

    for status in [
        TaskStatus.VALIDATED,
        TaskStatus.QUEUED,
        TaskStatus.DISPATCHED,
        TaskStatus.RUNNING,
    ]:
        manager.transition(task, status)

    manager.transition(task, TaskStatus.CANCELLED)

    assert task.status == TaskStatus.CANCELLED