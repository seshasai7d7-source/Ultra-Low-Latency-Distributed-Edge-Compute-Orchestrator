
from typing import ClassVar

from edge_orchestrator.models.task import Task, TaskStatus


class InvalidTaskTransition(Exception):
    """Raised when a task attempts an invalid state transition."""


class TaskLifecycleManager:
    """Controls valid task state transitions."""

    ALLOWED_TRANSITIONS: ClassVar[dict[TaskStatus, set[TaskStatus]]] = {
        TaskStatus.PENDING: {TaskStatus.VALIDATED, TaskStatus.CANCELLED},
        TaskStatus.VALIDATED: {TaskStatus.QUEUED, TaskStatus.CANCELLED},
        TaskStatus.QUEUED: {TaskStatus.DISPATCHED, TaskStatus.CANCELLED},
        TaskStatus.DISPATCHED: {TaskStatus.RUNNING, TaskStatus.CANCELLED},
        TaskStatus.RUNNING: {
            TaskStatus.COMPLETED,
            TaskStatus.FAILED,
            TaskStatus.TIMED_OUT,
            TaskStatus.CANCELLED,
        },
        TaskStatus.COMPLETED: set(),
        TaskStatus.FAILED: set(),
        TaskStatus.TIMED_OUT: set(),
        TaskStatus.CANCELLED: set(),
    }

    def transition(self, task: Task, new_status: TaskStatus) -> Task:
        allowed = self.ALLOWED_TRANSITIONS[task.status]

        if new_status not in allowed:
            raise InvalidTaskTransition(
                f"Cannot transition task from {task.status.value} "
                f"to {new_status.value}"
            )

        task.status = new_status
        return task
