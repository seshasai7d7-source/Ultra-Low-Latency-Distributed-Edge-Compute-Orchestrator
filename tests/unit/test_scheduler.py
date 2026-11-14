
import pytest

from edge_orchestrator.core.scheduler import (
    SchedulerError,
    TaskScheduler,
)
from edge_orchestrator.models.task import Task, TaskStatus


def validated_task(code: str = "result = 1") -> Task:
    return Task(code=code, status=TaskStatus.VALIDATED)


def test_submit_validated_task():
    scheduler = TaskScheduler()
    task = validated_task()

    scheduled = scheduler.submit(task)

    assert scheduled.status == TaskStatus.QUEUED
    assert scheduler.pending_count() == 1


def test_unvalidated_task_is_rejected():
    scheduler = TaskScheduler()
    task = Task(code="result = 1")

    with pytest.raises(SchedulerError):
        scheduler.submit(task)

    assert scheduler.pending_count() == 0


def test_fifo_order_is_preserved():
    scheduler = TaskScheduler()

    first = validated_task("first = 1")
    second = validated_task("second = 2")
    third = validated_task("third = 3")

    scheduler.submit(first)
    scheduler.submit(second)
    scheduler.submit(third)

    assert scheduler.get_next().task_id == first.task_id
    assert scheduler.get_next().task_id == second.task_id
    assert scheduler.get_next().task_id == third.task_id


def test_get_next_marks_task_as_dispatched():
    scheduler = TaskScheduler()
    task = validated_task()

    scheduler.submit(task)
    dispatched = scheduler.get_next()

    assert dispatched.status == TaskStatus.DISPATCHED


def test_empty_scheduler_returns_none():
    scheduler = TaskScheduler()

    assert scheduler.get_next() is None


def test_pending_count_decreases_after_dispatch():
    scheduler = TaskScheduler()
    scheduler.submit(validated_task())
    scheduler.submit(validated_task("result = 2"))

    assert scheduler.pending_count() == 2

    scheduler.get_next()

    assert scheduler.pending_count() == 1


def test_duplicate_task_is_rejected():
    scheduler = TaskScheduler()
    task = validated_task()

    scheduler.submit(task)

    with pytest.raises(SchedulerError):
        scheduler.submit(task)


def test_scheduler_is_empty():
    scheduler = TaskScheduler()

    assert scheduler.is_empty() is True

    scheduler.submit(validated_task())

    assert scheduler.is_empty() is False

    scheduler.get_next()

    assert scheduler.is_empty() is True
