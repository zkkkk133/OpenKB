from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import time
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from threading import RLock
from typing import Any
from urllib.parse import urlparse

from openkb.agent.query import run_query
from openkb.cache import resolve_cache_enabled
from openkb.cli import SUPPORTED_EXTENSIONS, _find_kb_dir, _setup_llm_key, add_single_file
from openkb.config import DEFAULT_CONFIG, load_config
from openkb.log import append_log
from openkb.service.feishu import export_feishu_pdf, is_feishu_url


_KB_LOCKS: dict[str, RLock] = {}
_KB_LOCKS_LOCK = RLock()
_QUERY_CACHE_LOCK = RLock()


def resolve_kb_dir(project_path: str | None = None) -> Path:
    configured = (
        project_path
        or os.environ.get("OPENKB_SERVICE_KB_DIR")
        or os.environ.get("OPENKB_DIR")
    )
    if configured:
        kb_dir = Path(configured).expanduser().resolve()
        if not (kb_dir / ".openkb").is_dir():
            raise ValueError(f"Not a knowledge base: {kb_dir}")
        return kb_dir

    kb_dir = _find_kb_dir()
    if kb_dir is None:
        raise ValueError("No knowledge base found. Set projectPath or OPENKB_SERVICE_KB_DIR.")
    return kb_dir


def query_kb(
    question: str,
    project_path: str | None = None,
    *,
    use_cache: bool | None = None,
    cache_ttl_days: int | None = None,
) -> dict[str, Any]:
    kb_dir = resolve_kb_dir(project_path)
    config = load_config(kb_dir / ".openkb" / "config.yaml")
    _setup_llm_key(kb_dir)
    model = config.get("model", DEFAULT_CONFIG["model"])
    effective_use_cache = resolve_cache_enabled(use_cache)
    ttl_days = _normalize_cache_ttl_days(cache_ttl_days)
    kb_revision = _kb_revision(kb_dir)
    cache_key = _query_cache_key(kb_dir, model, question, kb_revision)

    if effective_use_cache and ttl_days > 0:
        cached = _read_query_cache(kb_dir, cache_key, kb_revision)
        if cached is not None:
            result = dict(cached["result"])
            result["cache"] = {
                "enabled": True,
                "hit": True,
                "key": cache_key,
                "ttlDays": ttl_days,
                "createdAt": cached.get("createdAt"),
                "expiresAt": cached.get("expiresAt"),
                "kbRevision": kb_revision,
            }
            return result

    answer = asyncio.run(run_query(question, kb_dir, model, stream=False))
    append_log(kb_dir / "wiki", "query", question)
    result = {
        "answer": answer,
        "question": question,
        "kbDir": str(kb_dir),
        "model": model,
        "cache": {
            "enabled": effective_use_cache,
            "hit": False,
            "key": cache_key if effective_use_cache else None,
            "ttlDays": ttl_days,
            "kbRevision": kb_revision,
        },
    }
    if effective_use_cache and ttl_days > 0:
        _write_query_cache(kb_dir, cache_key, kb_revision, result, ttl_days)
    return result


def ingest_paths(
    paths: list[Path],
    project_path: str | None = None,
    *,
    use_cache: bool | None = None,
) -> dict[str, Any]:
    kb_dir = resolve_kb_dir(project_path)
    effective_use_cache = resolve_cache_enabled(use_cache)
    added: list[dict[str, Any]] = []
    logs: list[str] = []

    with _kb_lock(kb_dir):
        for path in paths:
            buffer = StringIO()
            with redirect_stdout(buffer):
                add_single_file(path, kb_dir, use_cache=effective_use_cache)
            output = buffer.getvalue()
            logs.append(output)
            added.append({"path": str(path), "name": path.name, "log": output})

    return {
        "kbDir": str(kb_dir),
        "count": len(added),
        "files": added,
        "logs": logs,
        "cache": {"enabled": effective_use_cache, "scope": "duplicate-file"},
    }


def collect_source_paths(
    source: str,
    *,
    staging_dir: Path,
    recursive: bool = True,
) -> list[Path]:
    if _is_url(source):
        if is_feishu_url(source):
            return [export_feishu_pdf(source, staging_dir)]
        raise ValueError(f"Unsupported URL source: {source}")

    path = Path(source).expanduser().resolve()
    if path.is_file():
        _ensure_supported(path)
        return [path]
    if path.is_dir():
        pattern = "**/*" if recursive else "*"
        files = sorted(p for p in path.glob(pattern) if p.is_file() and p.suffix.lower() in SUPPORTED_EXTENSIONS)
        if not files:
            raise ValueError(f"No supported files found in folder: {path}")
        return files
    raise FileNotFoundError(str(path))


def write_text_source(title: str, content: str, staging_dir: Path) -> Path:
    staging_dir.mkdir(parents=True, exist_ok=True)
    safe_title = _safe_filename(title or "text")
    path = staging_dir / f"{Path(safe_title).stem or 'text'}.txt"
    path.write_text(content, encoding="utf-8")
    return path


def write_uploaded_files(files: list[dict[str, Any]], staging_dir: Path) -> list[Path]:
    staging_dir.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []
    for item in files:
        filename = item.get("filename") or Path(item.get("relativePath") or "upload").name
        relative = item.get("relativePath") or filename
        relative_path = _safe_relative_path(str(relative))
        if relative_path.name == "":
            relative_path = Path(_safe_filename(filename))
        target = (staging_dir / relative_path).resolve()
        if not target.is_relative_to(staging_dir.resolve()):
            raise ValueError(f"Unsafe upload path: {relative}")
        target.parent.mkdir(parents=True, exist_ok=True)
        raw = base64.b64decode(item["contentBase64"])
        target.write_bytes(raw)
        _ensure_supported(target)
        paths.append(target)
    return paths


def service_staging_dir(kb_dir: Path, job_id: str) -> Path:
    return kb_dir / ".openkb" / "service_uploads" / job_id


def _kb_lock(kb_dir: Path) -> RLock:
    key = str(kb_dir.resolve())
    with _KB_LOCKS_LOCK:
        lock = _KB_LOCKS.get(key)
        if lock is None:
            lock = RLock()
            _KB_LOCKS[key] = lock
        return lock


def _ensure_supported(path: Path) -> None:
    if path.suffix.lower() not in SUPPORTED_EXTENSIONS:
        raise ValueError(
            f"Unsupported file type: {path.suffix}. Supported: {', '.join(sorted(SUPPORTED_EXTENSIONS))}"
        )


def _is_url(value: str) -> bool:
    parsed = urlparse(value)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _safe_filename(value: str) -> str:
    safe = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in value.strip())
    return safe or "upload"


def _safe_relative_path(value: str) -> Path:
    rel = Path(value.replace("\\", "/"))
    if rel.is_absolute() or ".." in rel.parts:
        raise ValueError(f"Unsafe relative path: {value}")
    return rel


def _normalize_cache_ttl_days(value: int | None) -> int:
    if value is None:
        return 5
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 5


def _query_cache_path(kb_dir: Path) -> Path:
    return kb_dir / ".openkb" / "service_cache" / "queries.json"


def _query_cache_key(kb_dir: Path, model: str, question: str, kb_revision: str) -> str:
    payload = {
        "kbDir": str(kb_dir.resolve()),
        "kbRevision": kb_revision,
        "model": model,
        "question": question.strip(),
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _kb_revision(kb_dir: Path) -> str:
    parts: list[str] = []
    for rel in (Path(".openkb") / "hashes.json", Path("wiki") / "index.md"):
        path = kb_dir / rel
        if path.exists():
            stat = path.stat()
            parts.append(f"{rel.as_posix()}:{stat.st_mtime_ns}:{stat.st_size}")
        else:
            parts.append(f"{rel.as_posix()}:missing")
    raw = "|".join(parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _read_query_cache(kb_dir: Path, cache_key: str, kb_revision: str) -> dict[str, Any] | None:
    now = time.time()
    with _QUERY_CACHE_LOCK:
        data = _load_query_cache(kb_dir)
        entry = data.get(cache_key)
        if not isinstance(entry, dict):
            return None
        if entry.get("kbRevision") != kb_revision:
            return None
        expires_at = entry.get("expiresAt")
        if not isinstance(expires_at, (int, float)) or expires_at <= now:
            data.pop(cache_key, None)
            _save_query_cache(kb_dir, data)
            return None
        result = entry.get("result")
        if not isinstance(result, dict):
            return None
        return entry


def _write_query_cache(
    kb_dir: Path,
    cache_key: str,
    kb_revision: str,
    result: dict[str, Any],
    ttl_days: int,
) -> None:
    now = time.time()
    entry_result = dict(result)
    entry_result.pop("cache", None)
    with _QUERY_CACHE_LOCK:
        data = _load_query_cache(kb_dir)
        data[cache_key] = {
            "createdAt": now,
            "expiresAt": now + ttl_days * 86400,
            "kbRevision": kb_revision,
            "result": entry_result,
        }
        _save_query_cache(kb_dir, data)


def _load_query_cache(kb_dir: Path) -> dict[str, Any]:
    path = _query_cache_path(kb_dir)
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _save_query_cache(kb_dir: Path, data: dict[str, Any]) -> None:
    path = _query_cache_path(kb_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
