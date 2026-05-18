# Tests

Automated test suite for the Enterprise Lakehouse Platform.

## Structure

- `unit/` — Unit tests for Spark utilities, DAG logic, and Python modules
- `integration/` — End-to-end pipeline validation tests

## Running Tests

```bash
pytest tests/unit -v
pytest tests/integration -v
```

CI runs unit tests on every push. Integration tests require a running Docker environment.