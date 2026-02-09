# Databricks Asset Bundle Monorepo Plan (with Pyrefly + Ruff)

## Summary
Build a single-root Databricks Asset Bundle monorepo aligned to Databricks `pydabs` template conventions, with:
- one root `databricks.yml`
- Python-defined resources loaded from `resources`
- stable namespace mapping: `catalog = domain`, `schema = source`
- env targets: `dev`, `staging`, `prod`
- strict generated resources via `just build-job-resource` and `just build-pipeline-resource`
- quality gates: `ruff` (format + lint) and `pyrefly` (type check)
- CI deployment blast radius limited by path-based git diff (domain-scoped by default)

## Public Interfaces / Contracts
- `just build-job-resource spec=<path>`
  - Validates spec with strict Pydantic models.
  - Generates Python Job resource module in `resources/<domain>/<source>/jobs/`.
- `just build-pipeline-resource spec=<path>`
  - Validates spec with strict Pydantic models.
  - Generates Python Pipeline resource module in `resources/<domain>/<source>/pipelines/`.
- `just format`
  - `ruff format .`
- `just lint`
  - `ruff check .`
- `just lint-fix`
  - `ruff check --fix .`
- `just typecheck`
  - `pyrefly check --summarize-errors`
- `just check`
  - `ruff format --check . && ruff check . && pyrefly check --summarize-errors`
- Naming policy:
  - `{domain}_{source}_{layer}_{asset}`
- Data mapping policy:
  - `catalog = domain`
  - `schema = source`

## Implementation Plan

### 1. Root Bundle Baseline
- Create root `databricks.yml` following `pydabs` shape:
  - `python.venv_path: .venv`
  - `python.resources: ["resources:load_resources"]`
  - `include` paths for resource YAML compatibility
  - wheel artifact build section
  - targets: `dev`, `staging`, `prod`
- Add environment overlays:
  - `targets/dev.yml`
  - `targets/staging.yml`
  - `targets/prod.yml`
- Keep non-secret workspace settings in overlays; keep secrets/auth external (Databricks profiles/CI env vars).

### 2. Monorepo Structure (Template-Only Seed)
- Scaffold placeholder-first directories with `.gitkeep` and `__init__.py` where needed:
  - `resources/`, `resources/_generated/`, `resources/_models/`
  - `resources/<domain>/<source>/jobs/`
  - `resources/<domain>/<source>/pipelines/`
  - `src/<domain>/<source>/raw/`
  - `src/<domain>/<source>/base/`
  - `src/<domain>/<source>/staging/`
  - `src/<domain>/<source>/final/`
  - `specs/jobs/`, `specs/pipelines/`, `scripts/`, `tests/`
- Add `resources/__init__.py` loader using `load_resources_from_current_package_module()`.

### 3. Central Generator with Strict Schemas
- Implement one central generator module to:
  - parse job/pipeline specs
  - validate via strict Pydantic (`extra="forbid"`)
  - emit deterministic Python resource modules (`Job.from_dict`, `Pipeline.from_dict`)
- Enforce naming and mapping (`catalog/domain`, `schema/source`) during generation.

### 4. Tooling: Ruff + Pyrefly
- In `pyproject.toml`:
  - add dev dependencies: `ruff`, `pyrefly`
  - configure `[tool.ruff]` and `[tool.ruff.format]`
  - configure `[tool.pyrefly]` with `src` and `resources` search paths
- Document local workflow in README/docs:
  - `just format`
  - `just lint`
  - `just typecheck`
  - `just check`

### 5. Source Layer Conventions
- Define layer contract (`raw`, `base`, `staging`, `final`) in folder stubs/README.
- Ensure generated pipeline paths and library globs align to layer folders.
- Stay close to Databricks template conventions; defer enhancements.

### 6. CI/CD Blast Radius + Quality Gates
- Add pre-deploy quality job on PR/merge:
  - `ruff format --check .`
  - `ruff check .`
  - `pyrefly check --summarize-errors`
- Deploy jobs depend on quality success.
- Path-based git diff determines impacted domain/source scopes.
- Global changes (`databricks.yml`, shared generator/models/tooling config) trigger full deploy.

### 7. Documentation
- `README.md`: repo layout, generation workflow, quality commands.
- `docs/resource-generation.md`: spec schema + generation behavior.
- `docs/deployment-scope.md`: diff-to-scope logic and full-deploy triggers.

## Test Cases and Scenarios
- Valid spec passes and generates deterministic module output.
- Invalid spec fails (missing required fields, unknown keys, invalid layer/name).
- Generated resources map `catalog=domain`, `schema=source`.
- `ruff format --check` fails on formatting drift.
- `ruff check` fails on lint issues.
- `pyrefly check` fails on type regressions.
- CI scope selection deploys only impacted domain/source; global changes trigger full deploy.
- `databricks bundle validate -t dev|staging|prod` passes for scaffold.

## Acceptance Criteria
- Root bundle with `dev/staging/prod` targets and Python resource loader is present.
- Jobs/pipelines are generated only through `just build-*` commands from specs.
- Strict schema validation enforced before generation.
- Layered source structure (`raw/base/staging/final`) exists and is documented.
- Ruff + Pyrefly are enforced locally and in CI prior to deploy.
- Deployment blast radius is domain-scoped by default through git-diff detection.
- Bundle structure stays close to Databricks `pydabs` template conventions.

## Assumptions / Defaults Locked
- Seed scope: template-only (no concrete domain/source instances yet).
- Generator architecture: centralized generator + strict Pydantic schemas.
- CI blast radius: path-based git diff.
- Env config strategy: per-target non-secret overlays; secrets externalized.
- Naming strategy: `{domain}_{source}_{layer}_{asset}`.
- Type checker: `pyrefly`.
- Formatter/linter: `ruff` only.
