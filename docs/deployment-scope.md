# Deployment scope

CI computes deployment scope from changed paths and deploys only impacted domain/source pairs by default.

## Scope extraction

Changed files are mapped to scopes when paths match:

- `resources/<domain>/<source>/...`
- `src/<domain>/<source>/...`
- `specs/jobs/<domain>/<source>/...`
- `specs/pipelines/<domain>/<source>/...`

Multiple changed files across the same scope are deduplicated.

## Full deploy triggers

A full deploy is triggered when shared bundle/generation/tooling files change, including:

- `databricks.yml`
- `targets/**`
- `pyproject.toml`
- `justfile`
- `resources/_generated/**`
- `resources/_models/**`
- `scripts/generate_resource.py`
- `scripts/detect_deployment_scope.py`
- `.github/workflows/**`

## Workflow behavior

1. Run quality gates (`ruff format --check`, `ruff check`, `pyrefly check --summarize-errors`).
2. Detect changed scopes from git diff.
3. If a full trigger is present, run full deployment.
4. Otherwise, deploy only changed scopes.
