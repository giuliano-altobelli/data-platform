# data-platform

Databricks Asset Bundle monorepo using Python-defined resources with strict generation from specs.

## Layout

- `databricks.yml`: root bundle config (`pydabs`-style) with Python resource loading.
- `targets/`: environment overlays (`dev`, `staging`, `prod`).
- `resources/`: generated Databricks jobs/pipelines and generation models.
- `src/`: transformation code split by `raw`, `base`, `staging`, `final` layers.
- `specs/`: YAML input specs for jobs and pipelines.
- `scripts/`: generation and deployment-scope helpers.
- `tests/`: generator and scope unit tests.

## Commands

- `just build-job-resource spec=<path>`
- `just build-pipeline-resource spec=<path>`
- `just format`
- `just lint`
- `just lint-fix`
- `just typecheck`
- `just check`

## Policies

- Naming: `{domain}_{source}_{layer}_{asset}`
- Generated Databricks resource names include env suffix:
  - `<resource_name>_${bundle.target}`
- Namespace mapping:
  - `catalog = domain`
  - `schema = source`

## Local setup

1. Install `uv` and `just`.
2. Install dependencies: `uv sync --dev`
3. Run checks: `just check`

## Documentation

- `docs/resource-generation.md`
- `docs/deployment-scope.md`

## Starter continuous pipeline

- Spec: `specs/pipelines/template_domain/template_source/basic_continuous.yml`
- Generate: `just build-pipeline-resource spec=specs/pipelines/template_domain/template_source/basic_continuous.yml`

## Simple low-latency stream join job

- Job spec: `specs/jobs/template_domain/template_source/low_latency_stream_join_simple.yml`
- Source: `src/template_domain/template_source/staging/low_latency_stream_join_simple_job.py`
- Generate: `just build-job-resource spec=specs/jobs/template_domain/template_source/low_latency_stream_join_simple.yml`
- Monitoring:
  - Find `inputRowsPerSecond`: Job run -> streaming task -> query Metrics/Timelines.
  - In logs, monitor `STREAM_MONITOR` JSON lines for `input_rows_per_second`, `processed_rows_per_second`, `trigger_execution_ms`, and `state_rows_total`.
  - Alert conditions in code: `trigger_execution_ms > 45000` or `backlog_streak >= 6` (logged as `STREAM_MONITOR_ALERT`).
  - First triage: check source lag, then scale fixed workers (2 -> 4) and keep `spark.sql.shuffle.partitions` aligned (8 -> 16).

## Example full-stack DLT streaming pipeline

- Spec: `specs/pipelines/finance/erp/core_streaming_full_stack.yml`
- Generate: `just build-pipeline-resource spec=specs/pipelines/finance/erp/core_streaming_full_stack.yml`
- Validate (dev): `databricks bundle validate -t dev`
- Deploy (dev): `databricks bundle deploy -t dev`
- Source layers:
  - `src/finance/erp/raw/`
  - `src/finance/erp/base/`
  - `src/finance/erp/staging/`
  - `src/finance/erp/final/`
