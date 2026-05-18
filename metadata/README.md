# Metadata

OpenMetadata integration configs, lineage definitions, and business glossary.

## Structure

- `openmetadata/ingestion/` — YAML ingestion connector configs
- `openmetadata/glossary/` — Business glossary term definitions
- `lineage/` — Explicit lineage mapping files

## OpenMetadata

The governance stack runs at http://localhost:8585. Start with:

```bash
make up-governance
```