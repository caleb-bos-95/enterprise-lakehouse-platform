"""
spark.jobs.bronze.handlers.base
===============================

Abstract contract for a Bronze source handler.

A handler's single responsibility is to materialise a raw DataFrame from
its source system. It does **not**:

  * apply business logic,
  * enforce schemas (Silver's job),
  * write to Iceberg (the orchestrator's job).

This separation keeps handlers small, side-effect-free, and unit-testable
without a live Spark cluster - they are pure read functions.

Adding a new handler
--------------------
1. Subclass ``SourceHandler``.
2. Implement ``read(spark, config)`` returning a ``DataFrame``.
3. Register the class in ``handlers/__init__.py`` with a unique ``type``
   key. That key becomes a valid value for ``type:`` in ``sources.yml``.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - type-only import
    from pyspark.sql import DataFrame, SparkSession

from spark.utils.config_loader import SourceConfig


class SourceHandler(ABC):
    """Abstract base class for all Bronze source handlers."""

    #: Identifier used in the YAML registry ``type`` field. Subclasses
    #: must override this with a unique, lowercase token.
    type_key: str = ""

    #: When ``True``, ``input_file_name()`` is captured into the
    #: ``_source_file`` metadata column. File handlers should set this.
    captures_source_file: bool = False

    @abstractmethod
    def read(self, spark: "SparkSession", config: SourceConfig) -> "DataFrame":
        """
        Materialise a DataFrame from the source described by ``config``.

        Implementations should:

          * fail fast on missing required ``options`` keys,
          * never call ``.cache()`` - caching is the orchestrator's call,
          * leave column casing untouched (Bronze is verbatim).
        """
        raise NotImplementedError

    def describe(self) -> str:
        """One-line human-readable handler description for run logs."""
        return f"{self.__class__.__name__}(type_key={self.type_key!r})"
