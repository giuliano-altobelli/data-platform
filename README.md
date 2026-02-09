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
