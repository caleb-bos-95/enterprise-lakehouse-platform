# dbt Core — Transformation Layer

Modular SQL transformations across the Bronze / Silver / Gold medallion architecture.

## Structure

- `models/bronze/` — Source staging models (raw to typed)
- `models/silver/` — Conformed dimensions and facts
- `models/gold/` — Business KPIs and analytics marts
- `tests/` — Custom data tests
- `macros/` — Reusable SQL macros
- `seeds/` — Reference datasets (e.g. lookup tables)

## Running dbt

```bash
dbt run --target dev
dbt test --target dev
dbt docs generate && dbt docs serve
```