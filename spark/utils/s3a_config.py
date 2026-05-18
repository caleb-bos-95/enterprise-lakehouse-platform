"""
spark.utils.s3a_config
======================

Hadoop / S3A configuration factory for accessing MinIO (and, in production,
any S3-compatible object store) from Spark.

This module centralises every S3A tunable in one place so that environment
behaviour cannot drift between Bronze, Silver, and Gold jobs. The defaults
are conservative and production-safe; overrides flow exclusively through
environment variables, which is what container orchestrators and CI runners
already manage.

Engineering rationale
---------------------
Co-locating S3A configuration with the SparkSession factory (rather than
inlining it into each job) lets us evolve credential providers, retry
policy, and committer choice without touching ingestion logic.
"""

from __future__ import annotations

import os
from typing import Dict


def s3a_hadoop_conf(
    endpoint: str | None = None,
    access_key: str | None = None,
    secret_key: str | None = None,
    *,
    path_style_access: bool = True,
    ssl_enabled: bool = False,
) -> Dict[str, str]:
    """
    Build a dictionary of Hadoop properties for S3A access to MinIO.

    Parameters
    ----------
    endpoint
        S3 endpoint URL. Defaults to ``S3_ENDPOINT`` env var, then
        ``http://minio:9000`` for the local Compose stack.
    access_key, secret_key
        Credentials. Default to ``AWS_ACCESS_KEY_ID`` / ``AWS_SECRET_ACCESS_KEY``,
        which is what the Bitnami Spark image already injects.
    path_style_access
        Must remain ``True`` for MinIO. Virtual-host style is AWS-only.
    ssl_enabled
        Toggle for production endpoints. MinIO in dev runs plaintext.

    Returns
    -------
    dict[str, str]
        Hadoop properties suitable for ``SparkSession.conf.set`` or
        ``--conf spark.hadoop.*`` flags.
    """
    endpoint = endpoint or os.environ.get("S3_ENDPOINT", "http://minio:9000")
    access_key = access_key or os.environ.get("AWS_ACCESS_KEY_ID", "admin")
    secret_key = secret_key or os.environ.get("AWS_SECRET_ACCESS_KEY", "password123")

    return {
        "spark.hadoop.fs.s3a.endpoint": endpoint,
        "spark.hadoop.fs.s3a.access.key": access_key,
        "spark.hadoop.fs.s3a.secret.key": secret_key,
        "spark.hadoop.fs.s3a.path.style.access": str(path_style_access).lower(),
        "spark.hadoop.fs.s3a.connection.ssl.enabled": str(ssl_enabled).lower(),
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        # Retry / connection tuning — safe defaults for object storage
        "spark.hadoop.fs.s3a.connection.maximum": "100",
        "spark.hadoop.fs.s3a.attempts.maximum": "20",
        "spark.hadoop.fs.s3a.retry.limit": "10",
        # Committer: magic committer is the recommended S3A committer for
        # idempotent writes in Iceberg-managed tables.
        "spark.hadoop.fs.s3a.committer.name": "magic",
        "spark.hadoop.fs.s3a.committer.magic.enabled": "true",
    }
