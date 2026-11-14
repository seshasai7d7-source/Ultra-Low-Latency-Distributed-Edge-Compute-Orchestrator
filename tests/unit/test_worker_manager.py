
import pytest

from edge_orchestrator.core.worker_manager import (
    WorkerManager,
    WorkerManagerError,
    WorkerStatus,
)


def test_register_worker():
    manager = WorkerManager()

    worker = manager.register("worker-1")

    assert worker.worker_id == "worker-1"
    assert worker.status == WorkerStatus.IDLE
    assert manager.worker_count() == 1


def test_duplicate_worker_registration_rejected():
    manager = WorkerManager()
    manager.register("worker-1")

    with pytest.raises(WorkerManagerError):
        manager.register("worker-1")


def test_empty_worker_id_rejected():
    manager = WorkerManager()

    with pytest.raises(WorkerManagerError):
        manager.register("")


def test_get_worker():
    manager = WorkerManager()
    manager.register("worker-1")

    worker = manager.get_worker("worker-1")

    assert worker.worker_id == "worker-1"


def test_missing_worker_rejected():
    manager = WorkerManager()

    with pytest.raises(WorkerManagerError):
        manager.get_worker("unknown-worker")


def test_mark_worker_busy():
    manager = WorkerManager()
    manager.register("worker-1")

    worker = manager.mark_busy("worker-1")

    assert worker.status == WorkerStatus.BUSY


def test_mark_worker_idle():
    manager = WorkerManager()
    manager.register("worker-1")
    manager.mark_busy("worker-1")

    worker = manager.mark_idle("worker-1")

    assert worker.status == WorkerStatus.IDLE


def test_worker_cannot_be_marked_busy_twice():
    manager = WorkerManager()
    manager.register("worker-1")
    manager.mark_busy("worker-1")

    with pytest.raises(WorkerManagerError):
        manager.mark_busy("worker-1")


def test_available_workers():
    manager = WorkerManager()
    manager.register("worker-1")
    manager.register("worker-2")
    manager.mark_busy("worker-2")

    available = manager.available_workers()

    assert len(available) == 1
    assert available[0].worker_id == "worker-1"


def test_heartbeat_updates_timestamp():
    manager = WorkerManager()
    manager.register("worker-1")

    original = manager.get_worker("worker-1").last_heartbeat
    updated = manager.heartbeat("worker-1")

    assert updated.last_heartbeat >= original


def test_unregister_worker():
    manager = WorkerManager()
    manager.register("worker-1")

    manager.unregister("worker-1")

    assert manager.worker_count() == 0

    with pytest.raises(WorkerManagerError):
        manager.get_worker("worker-1")


def test_external_changes_do_not_modify_registered_worker():
    manager = WorkerManager()
    manager.register("worker-1")

    worker = manager.get_worker("worker-1")
    worker.status = WorkerStatus.BUSY

    assert manager.get_worker("worker-1").status == WorkerStatus.IDLE
