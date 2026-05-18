"""
spark.utils.config_loader
=========================

Loader and schema validator for the Bronze source registry (``sources.yml``).

Why a config-driven registry
----------------------------
Onboarding a new source must be a low-risk, code-light operation. The
registry replaces per-source Python files with a declarative YAML entry
that the framework ingests and dispatches. Engineers add a block; the
framework handles wiring, observability, and Iceberg DDL.

Validation
----------
The YAML is validated against ``sources.schema.json`` at load time using
``jsonschema``. Validation runs before any Spark work begins, so misconfig
fails fast with an actionable error rather than after a 20-second cluster
spin-up.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator

import yaml
from jsonschema import Draft202012Validator


_SCHEMA_FILENAME = "sources.schema.json"


# ---------------------------------------------------------------------------
# Typed view of a single source entry
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class SourceConfig:
    """Validated, frozen view of one entry from ``sources.yml``."""

    name: str
    type: str
    target_namespace: str
    target_table: str
    options: dict[str, Any] = field(default_factory=dict)
    partition_by: tuple[str, ...] = ()
    enabled: bool = True
    description: str | None = None
    owner: str | None = None
    tags: tuple[str, ...] = ()

    @property
    def fqn_suffix(self) -> str:
        """The ``namespace.table`` portion of the Iceberg identifier."""
        return f"{self.target_namespace}.{self.target_table}"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def load_source_registry(
    registry_path: str | Path,
    *,
    schema_path: str | Path | None = None,
    only: list[str] | None = None,
) -> list[SourceConfig]:
    """
    Load and validate the source registry.

    Parameters
    ----------
    registry_path
        Path to ``sources.yml``.
    schema_path
        Path to the JSON Schema. Defaults to ``sources.schema.json`` next
        to the registry file.
    only
        Optional allowlist of source names. Useful for running a single
        source during development or backfill.

    Returns
    -------
    list[SourceConfig]
        Validated, enabled source configurations, in registry order.

    Raises
    ------
    FileNotFoundError
        If the registry or schema files are missing.
    jsonschema.ValidationError
        If the registry fails schema validation.
    ValueError
        If ``only`` references unknown sources, or duplicate source names
        are detected.
    """
    registry_path = Path(registry_path)
    schema_path = Path(schema_path) if schema_path else registry_path.parent / _SCHEMA_FILENAME

    raw = _read_yaml(registry_path)
    _validate(raw, _read_json(schema_path))

    sources = [_build(entry) for entry in raw["sources"]]
    _check_unique_names(sources)

    if only is not None:
        wanted = set(only)
        known = {s.name for s in sources}
        unknown = wanted - known
        if unknown:
            raise ValueError(f"Unknown source(s) requested via --only: {sorted(unknown)}")
        sources = [s for s in sources if s.name in wanted]

    return [s for s in sources if s.enabled]


def iter_enabled_sources(sources: list[SourceConfig]) -> Iterator[SourceConfig]:
    """Yield only the enabled sources. Kept as a helper for readability."""
    return (s for s in sources if s.enabled)


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------
def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Source registry not found: {path}")
    with path.open("r", encoding="utf-8") as fp:
        return yaml.safe_load(fp) or {}


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Source registry schema not found: {path}")
    with path.open("r", encoding="utf-8") as fp:
        return json.load(fp)


def _validate(payload: dict[str, Any], schema: dict[str, Any]) -> None:
    Draft202012Validator(schema).validate(payload)


def _build(entry: dict[str, Any]) -> SourceConfig:
    return SourceConfig(
        name=entry["name"],
        type=entry["type"],
        target_namespace=entry["target"]["namespace"],
        target_table=entry["target"]["table"],
        options=dict(entry.get("options", {})),
        partition_by=tuple(entry.get("partition_by", []) or []),
        enabled=bool(entry.get("enabled", True)),
        description=entry.get("description"),
        owner=entry.get("owner"),
        tags=tuple(entry.get("tags", []) or []),
    )


def _check_unique_names(sources: list[SourceConfig]) -> None:
    seen: dict[str, int] = {}
    for s in sources:
        seen[s.name] = seen.get(s.name, 0) + 1
    duplicates = sorted(name for name, count in seen.items() if count > 1)
    if duplicates:
        raise ValueError(f"Duplicate source name(s) in registry: {duplicates}")
