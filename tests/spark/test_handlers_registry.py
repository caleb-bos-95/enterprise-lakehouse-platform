"""
Tests for the handler registry in ``spark.jobs.bronze.handlers``.

The registry is the public seam through which the framework discovers
which source types are supported. These tests guard the contract that:

* every concrete handler advertises a non-empty ``type_key``,
* ``get_handler`` returns an instance of the correct class,
* unknown keys raise an informative ``KeyError``.
"""

from __future__ import annotations

import pytest

from spark.jobs.bronze.handlers import (
    SourceHandler,
    available_handlers,
    get_handler,
)
from spark.jobs.bronze.handlers.file_handler import FileHandler
from spark.jobs.bronze.handlers.jdbc_handler import JdbcHandler


def test_available_handlers_includes_built_ins():
    keys = available_handlers()
    assert "file" in keys
    assert "jdbc" in keys


@pytest.mark.parametrize(
    "type_key,expected_cls",
    [("file", FileHandler), ("jdbc", JdbcHandler)],
)
def test_get_handler_returns_expected_class(type_key, expected_cls):
    handler = get_handler(type_key)
    assert isinstance(handler, expected_cls)
    assert isinstance(handler, SourceHandler)
    assert handler.type_key == type_key


def test_get_handler_unknown_lists_supported(tmp_path):
    with pytest.raises(KeyError) as exc:
        get_handler("kafka")
    # The error message should list the supported types to speed onboarding.
    msg = str(exc.value)
    assert "file" in msg and "jdbc" in msg


def test_every_handler_declares_type_key():
    """Sanity check: every registered handler advertises a non-empty key."""
    for key in available_handlers():
        assert key, "handler type_key must be a non-empty string"
        assert key.islower(), f"type_key {key!r} should be lowercase"
