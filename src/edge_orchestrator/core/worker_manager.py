
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from threading import RLock


class WorkerManagerError(Exception):
    """Raised when a worker management operation fails."""


class WorkerStatus(str, Enum):
    IDLE = "IDLE"
    BUSY = "BUSY"


@dataclass
class Worker:
    worker_id: str
    status: WorkerStatus = WorkerStatus.IDLE
    registered_at: datetime = field(
        default_factory=lambda: datetime.now(UTC)
    )
    last_heartbeat: datetime = field(
        default_factory=lambda: datetime.now(UTC)
    )


class WorkerManager:
    """Maintains worker registration and availability."""

    def __init__(self) -> None:
        self._workers: dict[str, Worker] = {}
        self._lock = RLock()

    def register(self, worker_id: str) -> Worker:
        with self._lock:
            if not worker_id or not worker_id.strip():
                raise WorkerManagerError("Worker ID cannot be empty")

            if worker_id in self._workers:
                raise WorkerManagerError(
                    f"Worker '{worker_id}' is already registered"
                )

            worker = Worker(worker_id=worker_id)
            self._workers[worker_id] = worker

            return deepcopy(worker)

    def unregister(self, worker_id: str) -> None:
        with self._lock:
            if worker_id not in self._workers:
                raise WorkerManagerError(
                    f"Worker '{worker_id}' was not found"
                )

            del self._workers[worker_id]

    def get_worker(self, worker_id: str) -> Worker:
        with self._lock:
            worker = self._workers.get(worker_id)

            if worker is None:
                raise WorkerManagerError(
                    f"Worker '{worker_id}' was not found"
                )

            return deepcopy(worker)

    def heartbeat(self, worker_id: str) -> Worker:
        with self._lock:
            worker = self._get_worker(worker_id)
            worker.last_heartbeat = datetime.now(UTC)

            return deepcopy(worker)

    def mark_busy(self, worker_id: str) -> Worker:
        with self._lock:
            worker = self._get_worker(worker_id)

            if worker.status != WorkerStatus.IDLE:
                raise WorkerManagerError(
                    f"Worker '{worker_id}' is not idle"
                )

            worker.status = WorkerStatus.BUSY
            return deepcopy(worker)

    def mark_idle(self, worker_id: str) -> Worker:
        with self._lock:
            worker = self._get_worker(worker_id)

            if worker.status != WorkerStatus.BUSY:
                raise WorkerManagerError(
                    f"Worker '{worker_id}' is not busy"
                )

            worker.status = WorkerStatus.IDLE
            return deepcopy(worker)

    def available_workers(self) -> list[Worker]:
        with self._lock:
            return [
                deepcopy(worker)
                for worker in self._workers.values()
                if worker.status == WorkerStatus.IDLE
            ]

    def worker_count(self) -> int:
        with self._lock:
            return len(self._workers)

    def _get_worker(self, worker_id: str) -> Worker:
        worker = self._workers.get(worker_id)

        if worker is None:
            raise WorkerManagerError(
                f"Worker '{worker_id}' was not found"
            )

        return worker
