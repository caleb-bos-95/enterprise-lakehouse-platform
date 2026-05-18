# Airflow Plugins

Custom operators, hooks, and sensors for the lakehouse platform.

## Planned Plugins

- `IcebergOperator` — write Spark job output to Iceberg tables
- `OpenMetadataLineageHook` — emit lineage events to OpenMetadata on DAG completion
- `GreatExpectationsSensor` — block downstream tasks until GE suite passes