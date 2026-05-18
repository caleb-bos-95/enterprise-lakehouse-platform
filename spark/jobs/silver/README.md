# Spark Silver Jobs

Cleansing and validation jobs. Reads Bronze Iceberg tables, applies type casting, deduplication, null handling, and writes to Silver.

## Conventions

- All fields must have explicit data types
- Duplicate rows are removed using defined business keys
- Great Expectations validations run after each Silver write