from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from threading import RLock
from typing import Any, Callable
from uuid import uuid4


TERMINAL_STATUSES = {"done", "failed"}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class QueueJob:
    """In-memory job state for a simple API queue."""

    id: str
    queue: str
    payload: dict[str, Any]
    status: str = "queued"
    created_at: str = field(default_factory=utc_now)
    updated_at: str = field(default_factory=utc_now)
    started_at: str | None = None
    finished_at: str | None = None
    result: dict[str, Any] | None = None
    error: str | None = None

    def public(self, *, queue_info: dict[str, Any] | None = None) -> dict[str, Any]:
        data: dict[str, Any] = {
            "id": self.id,
            "queue": self.queue,
            "status": self.status,
            "createdAt": self.created_at,
            "updatedAt": self.updated_at,
            "startedAt": self.started_at,
            "finishedAt": self.finished_at,
            "pollAfterMs": 1000 if self.status not in TERMINAL_STATUSES else 0,
            "payload": _safe_payload(self.payload),
        }
        if queue_info is not None:
            data["queueInfo"] = queue_info
            data["position"] = queue_info.get("position")
            data["ahead"] = queue_info.get("ahead")
        if self.result is not None:
            data["result"] = self.result
        if self.error:
            data["error"] = self.error
        return data


Worker = Callable[[QueueJob], dict[str, Any]]


class JobQueue:
    """Small in-memory FIFO-ish queue backed by a ThreadPoolExecutor."""

    def __init__(self, name: str, max_workers: int = 1) -> None:
        self.name = name
        self.max_workers = max(1, max_workers)
        self._executor = ThreadPoolExecutor(
            max_workers=self.max_workers,
            thread_name_prefix=f"openkb-{name}",
        )
        self._lock = RLock()
        self._jobs: dict[str, QueueJob] = {}
        self._order: list[str] = []

    def submit(self, payload: dict[str, Any], worker: Worker) -> QueueJob:
        job = QueueJob(id=uuid4().hex, queue=self.name, payload=payload)
        with self._lock:
            self._jobs[job.id] = job
            self._order.append(job.id)
        self._executor.submit(self._run, job.id, worker)
        return job

    def get(self, job_id: str) -> QueueJob | None:
        with self._lock:
            return self._jobs.get(job_id)

    def public(self, job_id: str) -> dict[str, Any] | None:
        with self._lock:
            if job_id not in self._jobs:
                return None
            return self._public_locked(job_id)

    def snapshot(self) -> list[dict[str, Any]]:
        with self._lock:
            return [self._public_locked(jid) for jid in self._order]

    def stats(self) -> dict[str, Any]:
        with self._lock:
            counts: dict[str, int] = {}
            for job in self._jobs.values():
                counts[job.status] = counts.get(job.status, 0) + 1
            return {
                "name": self.name,
                "maxWorkers": self.max_workers,
                "total": len(self._jobs),
                "counts": counts,
            }

    def _public_locked(self, job_id: str) -> dict[str, Any]:
        job = self._jobs[job_id]
        return job.public(queue_info=self._queue_info_locked(job_id))

    def _queue_info_locked(self, job_id: str) -> dict[str, Any]:
        job = self._jobs[job_id]
        running = [jid for jid in self._order if self._jobs[jid].status == "running"]
        queued = [jid for jid in self._order if self._jobs[jid].status == "queued"]
        active = running + queued

        if job.status == "queued":
            waiting_position = queued.index(job_id) + 1
            active_position = active.index(job_id) + 1
            ahead = active_position - 1
            position = active_position
        elif job.status == "running":
            waiting_position = 0
            active_position = 0
            ahead = 0
            position = 0
        else:
            waiting_position = None
            active_position = None
            ahead = 0
            position = None

        return {
            "queue": self.name,
            "status": job.status,
            "position": position,
            "activePosition": active_position,
            "waitingPosition": waiting_position,
            "ahead": ahead,
            "running": len(running),
            "waiting": len(queued),
            "maxWorkers": self.max_workers,
        }

    def _run(self, job_id: str, worker: Worker) -> None:
        with self._lock:
            job = self._jobs[job_id]
            job.status = "running"
            job.started_at = utc_now()
            job.updated_at = job.started_at
        try:
            result = worker(job)
        except Exception as exc:  # pragma: no cover - defensive boundary
            with self._lock:
                job.status = "failed"
                job.error = str(exc)
                job.finished_at = utc_now()
                job.updated_at = job.finished_at
            return

        with self._lock:
            job.status = "done"
            job.result = result
            job.finished_at = utc_now()
            job.updated_at = job.finished_at


def _safe_payload(payload: dict[str, Any]) -> dict[str, Any]:
    safe = dict(payload)
    if "files" in safe:
        safe["files"] = [
            {
                "filename": f.get("filename"),
                "relativePath": f.get("relativePath"),
                "mimeType": f.get("mimeType"),
            }
            for f in safe.get("files", [])
            if isinstance(f, dict)
        ]
    if "content" in safe:
        safe["content"] = f"<{len(str(safe['content']))} chars>"
    return safe
