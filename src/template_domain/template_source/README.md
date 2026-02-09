# Layer contract

The source structure is fixed to four transformation layers:

- `raw`: ingestion and persistence of source data with minimal transformation.
- `base`: type casting, deduplication, and basic quality checks.
- `staging`: joins and business logic shaping.
- `final`: curated outputs ready for downstream reporting and consumption.

Pipelines should point their `root_path` and libraries to one of these layer folders.
