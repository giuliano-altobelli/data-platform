---
name: build-databricks-pipeline
description: Create or update Databricks pipeline specs and generated bundle resources in this data-platform repository. Use when users ask to add, edit, or validate a pipeline under specs/pipelines, generate Python pipeline resources with just build-pipeline-resource, enforce naming {domain}_{source}_{layer}_{asset}, and keep catalog=domain plus schema=source mapping.
---

# Build Databricks Pipeline

Build a Databricks pipeline by producing a valid spec, generating the Python resource, and validating quality gates.

## Workflow

1. Confirm the pipeline inputs.
Required inputs: `domain`, `source`, `layer`, `asset`.
Optional inputs: `continuous`, `development`, `serverless`, `channel`, `configuration` key-values.

2. Create or update the pipeline spec.
Write specs to `specs/pipelines/<domain>/<source>/` and keep names in lowercase snake_case.
Prefer using `scripts/create_pipeline_spec.py` to avoid schema drift.

3. Generate the resource from the spec.
Run `just build-pipeline-resource spec=<spec-path>`.
Do not hand-edit generated files under `resources/<domain>/<source>/pipelines/`.

4. Verify output mapping and paths.
Confirm generated payload includes:
- `catalog: <domain>`
- `schema: <source>`
- `name: <domain>_<source>_<layer>_<asset>`
- `root_path: src/<domain>/<source>/<layer>`
- `libraries` aligned to `src/<domain>/<source>/<layer>/**`

5. Run quality checks.
Run `just check` when possible. If sandboxed `uv` fails due cache permissions, request elevated execution for `just` commands.

## Quick Commands

- Create/update spec without generation:
```bash
python skills/build-databricks-pipeline/scripts/create_pipeline_spec.py \
  --domain finance \
  --source erp \
  --layer staging \
  --asset customer_360 \
  --continuous \
  --development \
  --config source_system=erp
```

- Create/update spec and generate resource:
```bash
python skills/build-databricks-pipeline/scripts/create_pipeline_spec.py \
  --domain finance \
  --source erp \
  --layer staging \
  --asset customer_360 \
  --continuous \
  --development \
  --build
```

## References

- Read `references/pipeline-specs.md` for field semantics, defaults, and a minimal continuous example.
