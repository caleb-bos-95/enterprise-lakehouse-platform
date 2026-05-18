"""
Validate the *committed* sources.yml against its schema.

This is the safety net that prevents a malformed registry from reaching
``main``. CI runs the same test on every PR.
"""

from __future__ import annotations

from pathlib import Path

from spark.utils.config_loader import load_source_registry


REPO_ROOT = Path(__file__).resolve().parents[2]
REGISTRY = REPO_ROOT / "spark" / "configs" / "sources" / "sources.yml"


def test_committed_registry_validates():
    # Load with ``only`` set to all source names so disabled entries are
    # exercised by the schema too.
    sources = load_source_registry(REGISTRY)
    # At least one source must be enabled in the committed registry.
    assert sources, "Committed sources.yml has no enabled sources."
