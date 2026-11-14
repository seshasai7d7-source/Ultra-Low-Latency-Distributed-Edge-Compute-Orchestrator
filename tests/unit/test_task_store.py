
from uuid import uuid4

import pytest

from edge_orchestrator.core.lifecycle import InvalidTaskTransition
from edge_orchestrator.core.task_store import (
    DuplicateTaskError,
    TaskNotFoundError,
    TaskStore,
    TaskStoreFullError,
)
from edge_orchestrator.models.task import Task, TaskStatus


def complete_task(store: TaskStore, task: Task) -> None:
    for status in [
        TaskStatus.VALIDATED,
        TaskStatus.QUEUED,
        TaskStatus.DISPATCHED,
        TaskStatus.RUNNING,
        TaskStatus.COMPLETED,
    ]:
        store.transition(task.task_id, status)


def test_add_and_get_task():
    store = TaskStore()
    task = Task(code="print(1)")

    store.add(task)
    retrieved = store.get(task.task_id)

    assert retrieved.task_id == task.task_id
    assert retrieved.code == "print(1)"


def test_duplicate_task_rejected():
    store = TaskStore()
    task = Task(code="print(1)")

    store.add(task)

    with pytest.raises(DuplicateTaskError):
        store.add(task)


def test_missing_task_rejected():
    store = TaskStore()

    with pytest.raises(TaskNotFoundError):
        store.get(uuid4())


def test_transition_updates_stored_task():
    store = TaskStore()
    task = Task(code="print(1)")
    store.add(task)

    updated = store.transition(task.task_id, TaskStatus.VALIDATED)

    assert updated.status == TaskStatus.VALIDATED
    assert store.get(task.task_id).status == TaskStatus.VALIDATED


def test_external_changes_do_not_modify_stored_task():
    store = TaskStore()
    task = Task(code="print(1)")
    store.add(task)

    retrieved = store.get(task.task_id)
    retrieved.status = TaskStatus.COMPLETED

    assert store.get(task.task_id).status == TaskStatus.PENDING


def test_oldest_terminal_task_is_evicted():
    store = TaskStore(capacity=2)

    first = Task(code="print(1)")
    second = Task(code="print(2)")
    third = Task(code="print(3)")

    store.add(first)
    store.add(second)
    complete_task(store, first)

    store.add(third)

    with pytest.raises(TaskNotFoundError):
        store.get(first.task_id)

    assert store.get(second.task_id).status == TaskStatus.PENDING
    assert store.get(third.task_id).status == TaskStatus.PENDING


def test_active_tasks_are_never_evicted():
    store = TaskStore(capacity=1)
    first = Task(code="print(1)")
    second = Task(code="print(2)")

    store.add(first)

    with pytest.raises(TaskStoreFullError):
        store.add(second)

    assert store.get(first.task_id).status == TaskStatus.PENDING


def test_task_counts():
    store = TaskStore()
    first = Task(code="print(1)")
    second = Task(code="print(2)")

    store.add(first)
    store.add(second)

    assert store.count() == 2
    assert store.active_count() == 2

    complete_task(store, first)

    assert store.count() == 2
    assert store.active_count() == 1


def test_invalid_transition_does_not_change_task():
    store = TaskStore()
    task = Task(code="print(1)")
    store.add(task)

    with pytest.raises(InvalidTaskTransition):
        store.transition(task.task_id, TaskStatus.RUNNING)

    assert store.get(task.task_id).status == TaskStatus.PENDING
