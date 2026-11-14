from uuid import uuid4

import pytest
import zmq
import zmq.asyncio

from edge_orchestrator.messaging.zeromq_transport import (
    ZeroMQTransport,
    ZeroMQTransportError,
)
from edge_orchestrator.models.message import Message, MessageType


@pytest.fixture
async def router_setup():
    context = zmq.asyncio.Context.instance()
    router = context.socket(zmq.ROUTER)
    endpoint = f"inproc://test-messaging-{uuid4()}"

    router.bind(endpoint)

    transport = ZeroMQTransport(
        endpoint,
        identity="test-worker",
    )

    yield router, transport

    await transport.close()
    router.close(linger=0)


@pytest.mark.asyncio
async def test_send_message(router_setup):
    router, transport = router_setup

    message = Message(
        message_type=MessageType.WORKER_HEARTBEAT,
        sender_id="test-worker",
    )

    await transport.send(message)

    identity, raw_message = await router.recv_multipart()
    restored = Message.model_validate_json(raw_message)

    assert identity == b"test-worker"
    assert restored.message_id == message.message_id


@pytest.mark.asyncio
async def test_receive_message(router_setup):
    router, transport = router_setup

    message = Message(
        message_type=MessageType.TASK_DISPATCH,
        sender_id="master",
    )

    await router.send_multipart([
        b"test-worker",
        message.model_dump_json().encode("utf-8"),
    ])

    received = await transport.receive(timeout_seconds=2)

    assert received is not None
    assert received.message_id == message.message_id


@pytest.mark.asyncio
async def test_receive_timeout(router_setup):
    _, transport = router_setup

    received = await transport.receive(timeout_seconds=0.05)

    assert received is None


@pytest.mark.asyncio
async def test_close_transport(router_setup):
    _, transport = router_setup

    await transport.close()

    assert transport._closed is True


@pytest.mark.asyncio
async def test_send_after_close(router_setup):
    _, transport = router_setup

    await transport.close()

    message = Message(
        message_type=MessageType.WORKER_HEARTBEAT,
        sender_id="test-worker",
    )

    with pytest.raises(ZeroMQTransportError):
        await transport.send(message)


def test_empty_endpoint_rejected():
    with pytest.raises(ValueError):
        ZeroMQTransport("")
