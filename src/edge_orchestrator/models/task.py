from datetime import UTC, datetime
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator


class TaskStatus(str, Enum):
    PENDING = "PENDING"
    VALIDATED = "VALIDATED"
    QUEUED = "QUEUED"
    DISPATCHED = "DISPATCHED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    TIMED_OUT = "TIMED_OUT"
    CANCELLED = "CANCELLED"


class Task(BaseModel):
    model_config = ConfigDict(validate_assignment=True)

    task_id: UUID = Field(default_factory=uuid4)
    code: str
    input_data: dict[str, Any] = Field(default_factory=dict)

    status: TaskStatus = TaskStatus.PENDING

    timeout_seconds: float = Field(default=30.0, gt=0)
    max_retries: int = Field(default=0, ge=0)

    created_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC)
    )

    @field_validator("code")
    @classmethod
    def validate_code(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("Task code cannot be empty")
        return value