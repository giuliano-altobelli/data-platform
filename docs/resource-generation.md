# Resource generation

Resources are generated from strict YAML specs and committed as Python modules.

## Commands

- Generate a job resource:
  `just build-job-resource spec=specs/jobs/<path>.yml`
- Generate a pipeline resource:
  `just build-pipeline-resource spec=specs/pipelines/<path>.yml`

Generated modules are written to:

- `resources/<domain>/<source>/jobs/<domain>_<source>_<layer>_<asset>.py`
- `resources/<domain>/<source>/pipelines/<domain>_<source>_<layer>_<asset>.py`

## Naming and namespace policy

- Resource name format: `{domain}_{source}_{layer}_{asset}`
- Namespace mapping is enforced in generated payloads:
  - `catalog = domain`
  - `schema = source`

## Job spec schema

```yaml
domain: finance
source: erp
layer: raw
asset: ingest_transactions

# Optional
description: Ingest ERP raw transactions
max_concurrent_runs: 1
tags:
  owner: data-platform
parameters:
  - name: refresh_window_days
    default: "7"

tasks:
  - task_key: ingest
    notebook_task:
      notebook_path: src/finance/erp/raw/ingest_transactions.ipynb
```

## Pipeline spec schema

```yaml
domain: finance
source: erp
layer: staging
asset: customer_360

# Optional
continuous: false
channel: CURRENT
configuration:
  quality: standard
libraries:
  - glob:
      include: src/finance/erp/staging/**
```

If `libraries` is omitted, generation defaults to:

```yaml
libraries:
  - glob:
      include: src/<domain>/<source>/<layer>/**
```

Any unknown field fails validation (`extra = forbid`).
