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
pipeline_kind: single_layer

# Optional
continuous: false
channel: CURRENT
configuration:
  quality: standard
libraries:
  - glob:
      include: src/finance/erp/staging/**
```

`pipeline_kind` controls root and library defaults:

- `single_layer` (default):
  - `layer` must be `raw|base|staging|final`
  - `root_path` is `src/<domain>/<source>/<layer>`
  - default libraries:

```yaml
libraries:
  - glob:
      include: src/<domain>/<source>/<layer>/**
```

- `full_stack`:
  - `layer` must be `full`
  - `root_path` is `src/<domain>/<source>`
  - default libraries:

```yaml
libraries:
  - glob: { include: src/<domain>/<source>/raw/** }
  - glob: { include: src/<domain>/<source>/base/** }
  - glob: { include: src/<domain>/<source>/staging/** }
  - glob: { include: src/<domain>/<source>/final/** }
```

Any unknown field fails validation (`extra = forbid`).

## Template continuous pipeline

A starter continuous pipeline spec is available at:

- `specs/pipelines/template_domain/template_source/basic_continuous.yml`

Generate it with:

- `just build-pipeline-resource spec=specs/pipelines/template_domain/template_source/basic_continuous.yml`

## Full-stack DLT streaming example

A tightly-coupled raw/base/staging/final streaming example is available at:

- `specs/pipelines/finance/erp/core_streaming_full_stack.yml`

Generate it with:

- `just build-pipeline-resource spec=specs/pipelines/finance/erp/core_streaming_full_stack.yml`

The generated module is:

- `resources/finance/erp/pipelines/finance_erp_full_core_streaming.py`

For this example, validate and deploy against dev target:

- `databricks bundle validate -t dev`
- `databricks bundle deploy -t dev`
