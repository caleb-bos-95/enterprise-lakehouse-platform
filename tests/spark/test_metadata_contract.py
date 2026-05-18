"""
Contract test for ``spark.utils.metadata.INGESTION_COLUMNS``.

This test does not require a SparkSession. Its purpose is to lock the
ingestion metadata schema in place — Silver, Gold, OpenMetadata configs,
and downstream contracts all depend on this exact tuple. If a column is
added or removed, the test must be updated deliberately as part of the
schema-evolution review.
"""

from __future__ import annotations

from spark.utils.metadata import INGESTION_COLUMNS


EXPECTED_INGESTION_COLUMNS = (
    "_ingested_at",
    "_source",
    "_source_type",
    "_batch_id",
    "_source_file",
)


def test_ingestion_columns_match_contract():
    assert INGESTION_COLUMNS == EXPECTED_INGESTION_COLUMNS, (
        "Ingestion metadata schema changed. Bronze->Silver consumers and "
        "OpenMetadata configs depend on this contract — update both sides "
        "before changing this list."
    )


def test_all_columns_underscore_prefixed():
    """Sanity check: ingestion metadata columns are namespaced with `_`."""
    for col in INGESTION_COLUMNS:
        assert col.startswith("_"), (
            f"Ingestion metadata column {col!r} must start with `_` to "
            f"avoid collisions with raw source columns."
        )
