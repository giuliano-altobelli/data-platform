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
