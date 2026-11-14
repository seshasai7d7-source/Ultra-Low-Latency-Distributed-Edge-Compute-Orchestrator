
from threading import RLock
from typing import ClassVar
from uuid import UUID

from edge_orchestrator.core.lifecycle import TaskLifecycleManager
from edge_orchestrator.models.task import Task, TaskStatus


class TaskNotFoundError(Exception):
    """Raised when a task cannot be found."""


class DuplicateTaskError(Exception):
    """Raised when a duplicate task is added."""


class TaskStoreFullError(Exception):
    """Raised when the task store has no available capacity."""


class TaskStore:
    """Thread-safe in-memory task repository."""

    TERMINAL_STATUSES: ClassVar[set[TaskStatus]] = {
        TaskStatus.COMPLETED,
        TaskStatus.FAILED,
        TaskStatus.TIMED_OUT,
        TaskStatus.CANCELLED,
    }

    def __init__(self, capacity: int = 10_000) -> None:
        if capacity < 1:
            raise ValueError("Capacity must be at least 1")

        self.capacity = capacity
        self._tasks: dict[UUID, Task] = {}
        self._lifecycle = TaskLifecycleManager()
        self._lock = RLock()

    def add(self, task: Task) -> Task:
        with self._lock:
            if task.task_id in self._tasks:
                raise DuplicateTaskError(
                    f"Task {task.task_id} already exists"
                )

            if len(self._tasks) >= self.capacity:
                self._evict_oldest_terminal()

            self._tasks[task.task_id] = task.model_copy(deep=True)
            return task.model_copy(deep=True)

    def get(self, task_id: UUID) -> Task:
        with self._lock:
            task = self._tasks.get(task_id)

            if task is None:
                raise TaskNotFoundError(
                    f"Task {task_id} was not found"
                )

            return task.model_copy(deep=True)

    def transition(
        self,
        task_id: UUID,
        new_status: TaskStatus,
    ) -> Task:
        with self._lock:
            task = self._tasks.get(task_id)

            if task is None:
                raise TaskNotFoundError(
                    f"Task {task_id} was not found"
                )

            self._lifecycle.transition(task, new_status)
            return task.model_copy(deep=True)

    def list_tasks(self) -> list[Task]:
        with self._lock:
            return [
                task.model_copy(deep=True)
                for task in self._tasks.values()
            ]

    def count(self) -> int:
        with self._lock:
            return len(self._tasks)

    def active_count(self) -> int:
        with self._lock:
            return sum(
                task.status not in self.TERMINAL_STATUSES
                for task in self._tasks.values()
            )

    def _evict_oldest_terminal(self) -> None:
        for task_id, task in self._tasks.items():
            if task.status in self.TERMINAL_STATUSES:
                del self._tasks[task_id]
                return

        raise TaskStoreFullError(
            "Task store is full and contains no evictable terminal tasks"
        )
