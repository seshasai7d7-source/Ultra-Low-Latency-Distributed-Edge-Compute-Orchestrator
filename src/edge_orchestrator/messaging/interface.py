
from typing import Protocol, runtime_checkable

from edge_orchestrator.models.message import Message


@runtime_checkable
class MessagingInterface(Protocol):
    """Contract for asynchronous message transports."""

    async def send(self, message: Message) -> None:
        """Send a message to the connected endpoint."""
        ...

    async def receive(
        self,
        timeout_seconds: float | None = None,
    ) -> Message | None:
        """Receive a message, or return None when the timeout expires."""
        ...

    async def close(self) -> None:
        """Release transport resources."""
        ...
