# Pipeline specs

## Required fields

- `domain`: lowercase snake_case data domain.
- `source`: lowercase snake_case source system.
- `layer`: one of `raw`, `base`, `staging`, `final`, `full`.
- `asset`: lowercase snake_case asset identifier.

Generated name format: `{domain}_{source}_{layer}_{asset}`.
Generated Databricks resource display name:
`{domain}_{source}_{layer}_{asset}_${bundle.target}`.

## Optional fields

- `pipeline_kind`: `single_layer` (default) or `full_stack`.
- `continuous`: enable continuous mode.
- `development`: development-mode behavior.
- `serverless`: serverless compute toggle.
- `channel`: release channel, typically `CURRENT`.
- `configuration`: map of string key-values.
- `libraries`: default should point to `src/<domain>/<source>/<layer>/**`.

## Pipeline modes

- `single_layer`:
  - requires `layer` in `raw|base|staging|final`
  - `root_path = src/<domain>/<source>/<layer>`
  - default library glob aligns to the selected layer folder

- `full_stack`:
  - requires `layer: full`
  - `root_path = src/<domain>/<source>`
  - default library globs include all four layer folders

## Minimal continuous example

```yaml
domain: template_domain
source: template_source
layer: staging
asset: basic_continuous
continuous: true
development: true
serverless: true
channel: CURRENT
configuration:
  pipeline_mode: continuous
libraries:
  - glob:
      include: src/template_domain/template_source/staging/**
```

## Full-stack streaming example

```yaml
domain: finance
source: erp
layer: full
asset: core_streaming
pipeline_kind: full_stack
continuous: true
development: true
serverless: true
channel: CURRENT
configuration:
  finance_erp_input_path: /Volumes/finance/erp/raw/transactions
  finance_erp_schema_location: /Volumes/finance/erp/checkpoints/autoloader_schema
```

## Generation command

Use:

```bash
just build-pipeline-resource spec=specs/pipelines/<domain>/<source>/<file>.yml
```

The generated file appears in:

`resources/<domain>/<source>/pipelines/<domain>_<source>_<layer>_<asset>.py`
