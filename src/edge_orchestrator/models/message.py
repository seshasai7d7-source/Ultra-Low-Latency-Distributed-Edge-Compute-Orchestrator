
from datetime import UTC, datetime
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field


class MessageType(str, Enum):
    WORKER_REGISTER = "WORKER_REGISTER"
    WORKER_REGISTERED = "WORKER_REGISTERED"
    TASK_DISPATCH = "TASK_DISPATCH"
    TASK_ACKNOWLEDGED = "TASK_ACKNOWLEDGED"
    TASK_STARTED = "TASK_STARTED"
    TASK_RESULT = "TASK_RESULT"
    TASK_ERROR = "TASK_ERROR"
    WORKER_HEARTBEAT = "WORKER_HEARTBEAT"
    WORKER_SHUTDOWN = "WORKER_SHUTDOWN"


class Message(BaseModel):
    model_config = ConfigDict(validate_assignment=True)

    message_id: UUID = Field(default_factory=uuid4)
    message_type: MessageType
    sender_id: str
    receiver_id: str | None = None

    correlation_id: UUID | None = None
    payload: dict[str, Any] = Field(default_factory=dict)

    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(UTC)
    )
