"""
Shared pytest fixtures for spark/ tests.

These tests are deliberately framework-only — they exercise the
config loader, handler registry, and metadata enrichment without
spinning up a SparkSession. Spark-integration tests live separately
(``tests/spark/integration/``) and are gated on a ``SPARK_HOME``
environment variable so CI can opt in selectively.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Add the repo root to sys.path so ``import spark.utils.config_loader``
# resolves without installing the package. Mirrors how spark-submit
# discovers modules when --py-files points at the spark/ directory.
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
