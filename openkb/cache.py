from __future__ import annotations

import os
from contextlib import contextmanager
from contextvars import ContextVar
from collections.abc import Iterator


_FALSE_VALUES = {"0", "false", "no", "off", "disable", "disabled"}
_TRUE_VALUES = {"1", "true", "yes", "on", "enable", "enabled"}
_CACHE_OVERRIDE: ContextVar[bool | None] = ContextVar("openkb_cache_override", default=None)


def resolve_cache_enabled(value: bool | None = None, *, default: bool = True) -> bool:
    """Resolve cache behavior from an explicit value or OPENKB cache env vars."""
    if value is not None:
        return bool(value)

    override = _CACHE_OVERRIDE.get()
    if override is not None:
        return override

    raw = os.environ.get("OPENKB_USE_CACHE")
    if raw is None:
        raw = os.environ.get("OPENKB_CACHE_ENABLED")
    if raw is None:
        return default

    normalized = raw.strip().lower()
    if normalized in _FALSE_VALUES:
        return False
    if normalized in _TRUE_VALUES:
        return True
    return default


@contextmanager
def cache_override(value: bool | None) -> Iterator[None]:
    token = _CACHE_OVERRIDE.set(value)
    try:
        yield
    finally:
        _CACHE_OVERRIDE.reset(token)
