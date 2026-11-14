
import inspect

from edge_orchestrator.messaging.interface import MessagingInterface


class FakeTransport:
    async def send(self, message):
        pass

    async def receive(self, timeout_seconds=None):
        return None

    async def close(self):
        pass


def test_fake_transport_implements_interface():
    transport = FakeTransport()

    assert isinstance(transport, MessagingInterface)


def test_interface_methods_are_async():
    assert inspect.iscoroutinefunction(MessagingInterface.send)
    assert inspect.iscoroutinefunction(MessagingInterface.receive)
    assert inspect.iscoroutinefunction(MessagingInterface.close)


def test_incomplete_transport_does_not_implement_interface():
    class IncompleteTransport:
        async def send(self, message):
            pass

    assert not isinstance(IncompleteTransport(), MessagingInterface)
