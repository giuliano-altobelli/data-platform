# Pipeline specs

## Required fields

- `domain`: lowercase snake_case data domain.
- `source`: lowercase snake_case source system.
- `layer`: one of `raw`, `base`, `staging`, `final`.
- `asset`: lowercase snake_case asset identifier.

Generated name format: `{domain}_{source}_{layer}_{asset}`.

## Optional fields

- `continuous`: enable continuous mode.
- `development`: development-mode behavior.
- `serverless`: serverless compute toggle.
- `channel`: release channel, typically `CURRENT`.
- `configuration`: map of string key-values.
- `libraries`: default should point to `src/<domain>/<source>/<layer>/**`.

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

## Generation command

Use:

```bash
just build-pipeline-resource spec=specs/pipelines/<domain>/<source>/<file>.yml
```

The generated file appears in:

`resources/<domain>/<source>/pipelines/<domain>_<source>_<layer>_<asset>.py`
