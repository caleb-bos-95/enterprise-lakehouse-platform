"""
spark.jobs.bronze.handlers.jdbc_handler
=======================================

Relational database source handler using Spark's JDBC reader.

This handler is the fallback path for any source that Airbyte cannot
service — niche systems, on-prem databases without connectors, or
controlled-network pulls where we deliberately want to avoid a long-lived
CDC stream.

Options contract
----------------

+---------------------+-----------+--------+-------------------------------+
| Key                 | Type      | Reqd?  | Notes                         |
+=====================+===========+========+===============================+
| url                 | string    | yes    | JDBC URL.                     |
| driver              | string    | yes    | Fully qualified driver class. |
| user_env            | string    | yes    | Env var name holding user.    |
| password_env        | string    | yes    | Env var name holding password.|
| dbtable             | string    | one of | Table or subquery to read.    |
| query               | string    | one of | Free-form SELECT statement.   |
| partition_column    | string    | no     | Numeric column for splits.    |
| lower_bound         | int       | no     | With partition_column.        |
| upper_bound         | int       | no     | With partition_column.        |
| num_partitions      | int       | no     | Parallelism for the read.     |
| fetch_size          | int       | no     | JDBC fetch size (default 1000)|
+---------------------+-----------+--------+-------------------------------+

Credentials are read from environment variables rather than the YAML
itself. The registry is checked into git; secrets are not.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover — type-only import
    from pyspark.sql import DataFrame, SparkSession

from spark.jobs.bronze.handlers.base import SourceHandler
from spark.utils.config_loader import SourceConfig


class JdbcHandler(SourceHandler):
    """Read a DataFrame from a JDBC-accessible relational source."""

    type_key = "jdbc"
    captures_source_file = False

    def read(self, spark: "SparkSession", config: SourceConfig) -> "DataFrame":
        opts = config.options
        url = _require(opts, "url", config.name)
        driver = _require(opts, "driver", config.name)
        user = _env(_require(opts, "user_env", config.name), config.name)
        password = _env(_require(opts, "password_env", config.name), config.name)

        if not (opts.get("dbtable") or opts.get("query")):
            raise ValueError(
                f"[{config.name}] jdbc handler requires either options.dbtable "
                f"or options.query."
            )
        if opts.get("dbtable") and opts.get("query"):
            raise ValueError(
                f"[{config.name}] jdbc handler accepts dbtable OR query, not both."
            )

        reader = (
            spark.read.format("jdbc")
            .option("url", url)
            .option("driver", driver)
            .option("user", user)
            .option("password", password)
            .option("fetchsize", int(opts.get("fetch_size", 1000)))
        )

        if "dbtable" in opts:
            reader = reader.option("dbtable", opts["dbtable"])
        else:
            reader = reader.option("query", opts["query"])

        # Optional parallel read configuration. All four properties must
        # be present together — Spark silently ignores them otherwise.
        partition_col = opts.get("partition_column")
        if partition_col:
            for required in ("lower_bound", "upper_bound", "num_partitions"):
                if required not in opts:
                    raise ValueError(
                        f"[{config.name}] jdbc partitioned read requires "
                        f"options.{required} alongside partition_column."
                    )
            reader = (
                reader
                .option("partitionColumn", partition_col)
                .option("lowerBound", str(opts["lower_bound"]))
                .option("upperBound", str(opts["upper_bound"]))
                .option("numPartitions", str(opts["num_partitions"]))
            )

        return reader.load()


def _require(options: dict[str, Any], key: str, source_name: str) -> Any:
    if key not in options or options[key] in (None, ""):
        raise ValueError(
            f"[{source_name}] jdbc handler requires options.{key} to be set."
        )
    return options[key]


def _env(var_name: str, source_name: str) -> str:
    value = os.environ.get(var_name)
    if not value:
        raise EnvironmentError(
            f"[{source_name}] environment variable {var_name!r} is unset; "
            f"jdbc credentials must be provided via env vars, never YAML."
        )
    return value
