
from collections import deque
from threading import RLock
from uuid import UUID

from edge_orchestrator.core.lifecycle import (
    TaskLifecycleManager,
)
from edge_orchestrator.models.task import Task, TaskStatus


class SchedulerError(Exception):
    """Raised when a scheduling operation fails."""


class TaskScheduler:
    """Thread-safe FIFO task scheduler."""

    def __init__(self) -> None:
        self._queue: deque[UUID] = deque()
        self._tasks: dict[UUID, Task] = {}
        self._lifecycle = TaskLifecycleManager()
        self._lock = RLock()

    def submit(self, task: Task) -> Task:
        with self._lock:
            if task.task_id in self._tasks:
                raise SchedulerError(
                    f"Task {task.task_id} is already scheduled"
                )

            if task.status != TaskStatus.VALIDATED:
                raise SchedulerError(
                    "Only validated tasks can be scheduled"
                )

            self._lifecycle.transition(task, TaskStatus.QUEUED)

            stored_task = task.model_copy(deep=True)
            self._tasks[task.task_id] = stored_task
            self._queue.append(task.task_id)

            return stored_task.model_copy(deep=True)

    def get_next(self) -> Task | None:
        with self._lock:
            if not self._queue:
                return None

            task_id = self._queue.popleft()
            task = self._tasks[task_id]

            self._lifecycle.transition(
                task,
                TaskStatus.DISPATCHED,
            )

            return task.model_copy(deep=True)

    def pending_count(self) -> int:
        with self._lock:
            return len(self._queue)

    def is_empty(self) -> bool:
        with self._lock:
            return not self._queue
