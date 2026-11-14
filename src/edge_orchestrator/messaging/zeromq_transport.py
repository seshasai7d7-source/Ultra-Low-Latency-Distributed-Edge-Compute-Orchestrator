
import asyncio

import zmq
import zmq.asyncio

from edge_orchestrator.messaging.interface import MessagingInterface
from edge_orchestrator.models.message import Message


class ZeroMQTransportError(RuntimeError):
    """Raised when the ZeroMQ transport is used incorrectly."""


class ZeroMQTransport(MessagingInterface):
    """Asynchronous ZeroMQ transport using a DEALER socket."""

    def __init__(
        self,
        endpoint: str,
        identity: str | None = None,
    ) -> None:
        if not endpoint:
            raise ValueError("Endpoint cannot be empty")

        self._context = zmq.asyncio.Context.instance()
        self._socket = self._context.socket(zmq.DEALER)

        if identity is not None:
            self._socket.setsockopt(zmq.IDENTITY, identity.encode("utf-8"))

        self._socket.connect(endpoint)
        self._closed = False

    async def send(self, message: Message) -> None:
        """Serialize and send a message."""
        if self._closed:
            raise ZeroMQTransportError("Transport is closed")

        await self._socket.send_string(message.model_dump_json())

    async def receive(
        self,
        timeout_seconds: float | None = None,
    ) -> Message | None:
        """Receive and deserialize a message."""
        if self._closed:
            raise ZeroMQTransportError("Transport is closed")

        if timeout_seconds is not None and timeout_seconds < 0:
            raise ValueError("Timeout cannot be negative")

        try:
            if timeout_seconds is None:
                raw_message = await self._socket.recv_string()
            else:
                raw_message = await asyncio.wait_for(
                    self._socket.recv_string(),
                    timeout=timeout_seconds,
                )

            return Message.model_validate_json(raw_message)

        except TimeoutError:
            return None

    async def close(self) -> None:
        """Close the socket and release its resources."""
        if not self._closed:
            self._socket.close(linger=0)
            self._closed = True
