"""
Tests for ``spark.utils.config_loader``.

These tests cover:

* Successful load of a valid registry,
* Filtering via the ``only`` allowlist,
* Schema validation rejection of malformed entries,
* Duplicate-name detection.
"""

from __future__ import annotations

import json
import textwrap
from pathlib import Path

import pytest
import yaml
from jsonschema import ValidationError

from spark.utils.config_loader import load_source_registry


SCHEMA_PATH = (
    Path(__file__).resolve().parents[2]
    / "spark" / "configs" / "sources" / "sources.schema.json"
)


def _write_registry(tmp_path: Path, payload: dict) -> Path:
    registry = tmp_path / "sources.yml"
    registry.write_text(yaml.safe_dump(payload), encoding="utf-8")
    # Co-locate the schema so the loader's default lookup succeeds.
    (tmp_path / "sources.schema.json").write_text(
        SCHEMA_PATH.read_text(encoding="utf-8"), encoding="utf-8",
    )
    return registry


def _valid_payload() -> dict:
    return {
        "version": 1,
        "sources": [
            {
                "name": "banksim_transactions",
                "type": "file",
                "owner": "data-platform-team",
                "target": {"namespace": "bronze", "table": "banksim_transactions"},
                "options": {
                    "path": "file:///opt/datasets/raw/bs.csv",
                    "format": "csv",
                    "reader_options": {"header": "true"},
                },
            },
            {
                "name": "postgres_orders",
                "type": "jdbc",
                "enabled": False,
                "target": {"namespace": "bronze", "table": "postgres_orders"},
                "options": {
                    "url": "jdbc:postgresql://postgres:5432/commerce",
                    "driver": "org.postgresql.Driver",
                    "user_env": "BRONZE_PG_USER",
                    "password_env": "BRONZE_PG_PASSWORD",
                    "dbtable": "public.orders",
                },
            },
        ],
    }


def test_load_valid_registry_returns_only_enabled(tmp_path):
    registry = _write_registry(tmp_path, _valid_payload())
    sources = load_source_registry(registry)
    assert [s.name for s in sources] == ["banksim_transactions"]
    assert sources[0].fqn_suffix == "bronze.banksim_transactions"
    assert sources[0].type == "file"


def test_only_filter_returns_specific_sources(tmp_path):
    payload = _valid_payload()
    # Enable both sources so the ``only`` filter does the narrowing.
    payload["sources"][1]["enabled"] = True
    registry = _write_registry(tmp_path, payload)
    sources = load_source_registry(registry, only=["postgres_orders"])
    assert [s.name for s in sources] == ["postgres_orders"]


def test_unknown_only_source_raises(tmp_path):
    registry = _write_registry(tmp_path, _valid_payload())
    with pytest.raises(ValueError, match="Unknown source"):
        load_source_registry(registry, only=["does_not_exist"])


def test_duplicate_names_rejected(tmp_path):
    payload = _valid_payload()
    payload["sources"].append(payload["sources"][0].copy())
    registry = _write_registry(tmp_path, payload)
    with pytest.raises(ValueError, match="Duplicate source"):
        load_source_registry(registry)


def test_missing_required_options_rejected_by_schema(tmp_path):
    payload = _valid_payload()
    # Strip the required `format` option from the file source.
    payload["sources"][0]["options"] = {"path": "file:///x.csv"}
    registry = _write_registry(tmp_path, payload)
    with pytest.raises(ValidationError):
        load_source_registry(registry)


def test_jdbc_requires_dbtable_or_query(tmp_path):
    payload = _valid_payload()
    payload["sources"][1]["enabled"] = True
    payload["sources"][1]["options"].pop("dbtable")
    registry = _write_registry(tmp_path, payload)
    with pytest.raises(ValidationError):
        load_source_registry(registry)


def test_invalid_source_name_rejected(tmp_path):
    payload = _valid_payload()
    payload["sources"][0]["name"] = "BadName"  # uppercase not allowed
    registry = _write_registry(tmp_path, payload)
    with pytest.raises(ValidationError):
        load_source_registry(registry)
