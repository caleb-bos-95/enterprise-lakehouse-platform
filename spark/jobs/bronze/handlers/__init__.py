"""
spark.jobs.bronze.handlers
==========================

Handler registry.

Each handler advertises a unique ``type_key`` that matches the ``type:``
field in ``sources.yml``. The registry is the only place where the
framework needs to know about the concrete handler set — adding a new
source type is a two-line change here plus one new module.

Public API
----------
- ``get_handler(type_key)`` — returns a handler instance for a registry key
- ``available_handlers()``  — returns the set of supported type keys
"""

from __future__ import annotations

from typing import Dict, Type

from spark.jobs.bronze.handlers.base import SourceHandler
from spark.jobs.bronze.handlers.file_handler import FileHandler
from spark.jobs.bronze.handlers.jdbc_handler import JdbcHandler


_HANDLER_CLASSES: tuple[Type[SourceHandler], ...] = (
    FileHandler,
    JdbcHandler,
)

_HANDLER_REGISTRY: Dict[str, Type[SourceHandler]] = {
    cls.type_key: cls for cls in _HANDLER_CLASSES
}


def get_handler(type_key: str) -> SourceHandler:
    """
    Return a new handler instance for ``type_key``.

    Raises
    ------
    KeyError
        If no handler is registered for the requested key. The error
        message lists the supported keys to speed up onboarding.
    """
    try:
        return _HANDLER_REGISTRY[type_key]()
    except KeyError as exc:
        supported = sorted(_HANDLER_REGISTRY.keys())
        raise KeyError(
            f"No handler registered for source type {type_key!r}. "
            f"Supported types: {supported}."
        ) from exc


def available_handlers() -> tuple[str, ...]:
    """Return the tuple of registered handler keys, sorted for stable output."""
    return tuple(sorted(_HANDLER_REGISTRY.keys()))


__all__ = ["get_handler", "available_handlers", "SourceHandler"]
