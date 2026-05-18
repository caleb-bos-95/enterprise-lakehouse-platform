"""
spark.jobs.bronze.handlers.file_handler
=======================================

File-based source handler covering CSV, JSON, and Parquet inputs.

Supports any URI that Hadoop FileSystem can resolve:

  * ``s3a://bucket/prefix/*.csv``    — MinIO landing zone or Airbyte output
  * ``file:///opt/datasets/raw/...`` — local development files
  * ``hdfs://...``                   — future HDFS interop

Options contract (set per source in ``sources.yml``)
----------------------------------------------------

+---------------------+-----------+--------+-------------------------------+
| Key                 | Type      | Reqd?  | Notes                         |
+=====================+===========+========+===============================+
| path                | string    | yes    | Glob-capable input URI.       |
| format              | string    | yes    | csv | json | parquet          |
| reader_options      | mapping   | no     | Passed through to Spark.      |
| recursive_lookup    | bool      | no     | Recurse into subdirectories.  |
+---------------------+-----------+--------+-------------------------------+

Anything under ``reader_options`` is handed verbatim to ``DataFrameReader``,
which keeps the handler future-proof against new Spark options without
code changes.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover — type-only import
    from pyspark.sql import DataFrame, SparkSession

from spark.jobs.bronze.handlers.base import SourceHandler
from spark.utils.config_loader import SourceConfig


_SUPPORTED_FORMATS = {"csv", "json", "parquet"}


class FileHandler(SourceHandler):
    """Read a DataFrame from a file-based source."""

    type_key = "file"
    captures_source_file = True

    def read(self, spark: "SparkSession", config: SourceConfig) -> "DataFrame":
        options = config.options
        path = _require(options, "path", config.name)
        fmt = _require(options, "format", config.name).lower()

        if fmt not in _SUPPORTED_FORMATS:
            raise ValueError(
                f"[{config.name}] unsupported file format: {fmt!r}. "
                f"Expected one of {sorted(_SUPPORTED_FORMATS)}."
            )

        reader_options: dict[str, Any] = dict(options.get("reader_options", {}) or {})
        if options.get("recursive_lookup"):
            reader_options.setdefault("recursiveFileLookup", "true")

        # Format-specific safe defaults — overridable by reader_options.
        if fmt == "csv":
            reader_options.setdefault("header", "true")
            reader_options.setdefault("inferSchema", "false")  # Bronze stays raw
            reader_options.setdefault("mode", "PERMISSIVE")
        elif fmt == "json":
            reader_options.setdefault("multiLine", "false")

        reader = spark.read.format(fmt)
        for key, value in reader_options.items():
            reader = reader.option(key, str(value))

        return reader.load(path)


def _require(options: dict[str, Any], key: str, source_name: str) -> Any:
    if key not in options or options[key] in (None, ""):
        raise ValueError(
            f"[{source_name}] file handler requires options.{key} to be set."
        )
    return options[key]
