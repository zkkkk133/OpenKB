from __future__ import annotations

import os
from pathlib import Path
from typing import Any
from uuid import uuid4

from openkb.service.jobs import JobQueue, QueueJob
from openkb.service.runtime import (
    collect_source_paths,
    ingest_paths,
    query_kb,
    resolve_kb_dir,
    service_staging_dir,
    write_text_source,
    write_uploaded_files,
)


class OpenKBService:
    """Facade for API queues and OpenKB operations."""

    def __init__(self) -> None:
        add_workers = int(os.environ.get("OPENKB_ADD_WORKERS", "2"))
        query_workers = int(os.environ.get("OPENKB_QUERY_WORKERS", "4"))
        self.knowledge_queue = JobQueue("knowledge", max_workers=add_workers)
        self.ask_queue = JobQueue("ask", max_workers=query_workers)

    def submit_query(self, payload: dict[str, Any]) -> QueueJob:
        return self.ask_queue.submit(payload, self._run_query)

    def submit_text(self, payload: dict[str, Any]) -> QueueJob:
        return self.knowledge_queue.submit(payload | {"mode": "text"}, self._run_knowledge)

    def submit_files(self, payload: dict[str, Any]) -> QueueJob:
        kb_dir = resolve_kb_dir(payload.get("projectPath"))
        staging_id = payload.get("stagingId") or uuid4().hex
        staging_dir = service_staging_dir(kb_dir, staging_id)
        paths = write_uploaded_files(payload.get("files") or [], staging_dir)
        clean_payload = dict(payload)
        clean_payload.pop("files", None)
        clean_payload["mode"] = "paths"
        clean_payload["paths"] = [str(p) for p in paths]
        return self.knowledge_queue.submit(clean_payload, self._run_knowledge)

    def submit_source(self, payload: dict[str, Any]) -> QueueJob:
        return self.knowledge_queue.submit(payload | {"mode": "source"}, self._run_knowledge)

    def ask_status(self, job_id: str) -> dict[str, Any] | None:
        return self.ask_queue.public(job_id)

    def knowledge_status(self, job_id: str) -> dict[str, Any] | None:
        return self.knowledge_queue.public(job_id)

    def queues(self) -> dict[str, Any]:
        return {
            "askQueue": self.ask_queue.snapshot(),
            "knowledgeQueue": self.knowledge_queue.snapshot(),
            "stats": {
                "ask": self.ask_queue.stats(),
                "knowledge": self.knowledge_queue.stats(),
            },
        }

    def _run_query(self, job: QueueJob) -> dict[str, Any]:
        payload = job.payload
        return query_kb(
            payload["question"],
            payload.get("projectPath"),
            use_cache=payload.get("useCache"),
            cache_ttl_days=payload.get("cacheTtlDays"),
        )

    def _run_knowledge(self, job: QueueJob) -> dict[str, Any]:
        payload = job.payload
        kb_dir = resolve_kb_dir(payload.get("projectPath"))
        staging_dir = service_staging_dir(kb_dir, job.id)
        mode = payload.get("mode")

        if mode == "text":
            path = write_text_source(payload.get("title") or "text", payload["content"], staging_dir)
            paths = [path]
        elif mode == "paths":
            paths = [Path(p) for p in payload.get("paths") or []]
        elif mode == "source":
            paths = collect_source_paths(
                payload["source"],
                staging_dir=staging_dir,
                recursive=bool(payload.get("recursive", True)),
            )
        else:
            raise ValueError(f"Unknown knowledge job mode: {mode}")

        return ingest_paths(paths, payload.get("projectPath"), use_cache=payload.get("useCache"))


SERVICE = OpenKBService()
