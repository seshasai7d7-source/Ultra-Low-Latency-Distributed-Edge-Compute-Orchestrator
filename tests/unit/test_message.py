
from uuid import UUID

from edge_orchestrator.models.message import Message, MessageType


def test_message_creation():
    message = Message(
        message_type=MessageType.WORKER_REGISTER,
        sender_id="worker-1",
    )

    assert message.message_type == MessageType.WORKER_REGISTER
    assert message.sender_id == "worker-1"


def test_message_id_is_generated():
    message = Message(
        message_type=MessageType.WORKER_REGISTER,
        sender_id="worker-1",
    )

    assert isinstance(message.message_id, UUID)


def test_message_payload():
    message = Message(
        message_type=MessageType.TASK_DISPATCH,
        sender_id="master",
        receiver_id="worker-1",
        payload={"task_id": "123"},
    )

    assert message.payload["task_id"] == "123"


def test_correlation_id():
    original = Message(
        message_type=MessageType.TASK_DISPATCH,
        sender_id="master",
    )

    response = Message(
        message_type=MessageType.TASK_RESULT,
        sender_id="worker-1",
        correlation_id=original.message_id,
    )

    assert response.correlation_id == original.message_id


def test_message_serialization():
    message = Message(
        message_type=MessageType.WORKER_HEARTBEAT,
        sender_id="worker-1",
    )

    serialized = message.model_dump_json()
    restored = Message.model_validate_json(serialized)

    assert restored.message_id == message.message_id
    assert restored.message_type == message.message_type


def test_message_timestamp():
    message = Message(
        message_type=MessageType.WORKER_REGISTER,
        sender_id="worker-1",
    )

    assert message.timestamp.tzinfo is not None


def test_message_without_receiver():
    message = Message(
        message_type=MessageType.WORKER_REGISTER,
        sender_id="worker-1",
    )

    assert message.receiver_id is None
