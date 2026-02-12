# Phase 1/2: Dev AWS Infra, Phase 2/2: Local Integration Tests (Separated)

## Summary
Split the work into two explicit phases so infra can be delivered independently of any test code.

- Phase 1 provisions a dev RDS Postgres (logical replication + wal2json) and a dev Kinesis Data Stream via
  Terraform under `services/infra/`.
- Phase 2 adds local-only integration tests for `services/src/cdc_logical_replication` that consume those
  Terraform outputs (no CI/CD).

## Phase 1: Terraform Dev Infra (deliverable 1)

### Goals
- Create AWS resources required for local integration tests:
  - Public dev RDS Postgres with logical replication enabled and a dedicated replication user
  - Dev Kinesis stream
- Provide stable, copy-paste-friendly outputs for local test configuration.
- Use AWS credentials from environment variables.

### Directory Layout (new)
- `services/infra/README.md`
- `services/infra/modules/kinesis_stream/main.tf`
- `services/infra/modules/kinesis_stream/variables.tf`
- `services/infra/modules/kinesis_stream/outputs.tf`
- `services/infra/modules/rds_postgres_logical_replication/main.tf`
- `services/infra/modules/rds_postgres_logical_replication/variables.tf`
- `services/infra/modules/rds_postgres_logical_replication/outputs.tf`
- `services/infra/stacks/cdc_logical_replication_dev/README.md`
- `services/infra/stacks/cdc_logical_replication_dev/versions.tf`
- `services/infra/stacks/cdc_logical_replication_dev/main.tf`
- `services/infra/stacks/cdc_logical_replication_dev/variables.tf`
- `services/infra/stacks/cdc_logical_replication_dev/outputs.tf`
- `services/infra/stacks/cdc_logical_replication_dev/bootstrap_db.sh`
- Repo `.gitignore` update to ignore:
  - `services/infra/**/.terraform/`
  - `services/infra/**/terraform.tfstate*`
  - `services/infra/**/.terraform.lock.hcl` (decide to ignore or commit; document choice)
  - `services/infra/**/*.tfvars`

### Locked Decisions (Phase 1)
- Networking: `publicly_accessible=true`; inbound `5432` restricted to `var.allowed_cidrs` only.
- AWS auth: env vars (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional `AWS_SESSION_TOKEN`, `AWS_REGION`).
- DB bootstrap: Terraform runs a local, idempotent bootstrap step post-create to create/grant a replication user.

### Stack Variables (`services/infra/stacks/cdc_logical_replication_dev/variables.tf`)
- `aws_region` (default `us-west-2`)
- `name_prefix` (default `cdc-lr-dev`)
- `tags` (default `{ project="data-platform", component="cdc-logical-replication", env="dev" }`)
- `allowed_cidrs` (required)
- `db_instance_class` (default `db.t3.micro`)
- `db_engine_version` (default pinned org-approved Postgres version)
- `db_allocated_storage_gb` (default `20`)
- `db_name` (default `cdc`)
- `db_master_username` (default `cdc_admin`)
- `replication_username` (default `cdc_replication`)
- `kinesis_shard_count` (default `1`)
- `kinesis_retention_hours` (default `24`)
- `use_default_vpc` (default `true`)
- `vpc_id`, `subnet_ids` (used if `use_default_vpc=false`)

### AWS Resources (Phase 1)
- Default VPC discovery (unless explicitly provided VPC/subnets)
- `aws_db_subnet_group`
- `aws_security_group` for Postgres:
  - inbound `5432` from `allowed_cidrs`
  - outbound all
- `aws_db_parameter_group` (e.g., `postgres15` family) with:
  - `rds.logical_replication=1` (`pending-reboot`)
  - `max_replication_slots=10`
  - `max_wal_senders=10`
- `aws_db_instance` (dev-friendly):
  - `engine=postgres`, `engine_version=...`
  - `publicly_accessible=true`
  - `multi_az=false`, `backup_retention_period=0`
  - `skip_final_snapshot=true`, `deletion_protection=false`
  - password via `random_password`
  - `apply_immediately=true`
- `aws_kinesis_stream` with `shard_count=1`, retention `24h`

### Bootstrap Step (Phase 1)
Add a `null_resource` with `local-exec` running `bootstrap_db.sh` after RDS is ready.

`bootstrap_db.sh` is idempotent and:
- connects with `sslmode=require`
- creates `replication_username` with `LOGIN REPLICATION PASSWORD ...` if missing
- grants `rds_replication` to that user if the role exists
- grants `CONNECT` on the target database

Implementation note: use Dockerized `psql` by default (document requirement: Docker installed), with a fallback
path if native `psql` exists.

### Outputs (Phase 1)
- `pg_host`, `pg_port`, `pg_database`
- `pg_master_user`, `pg_master_password` (sensitive)
- `pg_replication_user`, `pg_replication_password` (sensitive)
- `kinesis_stream_name`, `kinesis_stream_arn`
- `aws_region`
- Optional: `env_exports` (non-sensitive) to show exact env var names tests/service will use:
  - `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`
  - `AWS_REGION`, `KINESIS_STREAM`

### Phase 1 Acceptance Criteria
- `terraform apply` creates RDS + parameter group enabling logical replication, and a Kinesis stream.
- Post-apply, the replication user exists and can establish a replication connection and create/check a logical
  replication slot.
- Outputs provide everything needed to run tests locally.

### Checkpoint (Execution Log, Not Plan Changes)
- Date: February 12, 2026
- Status: Phase 1 implemented and applied successfully.
- Infra implemented under `services/infra/`:
  - modules: `kinesis_stream`, `rds_postgres_logical_replication`
  - stack: `stacks/cdc_logical_replication_dev`
  - docs/readme + `.gitignore` Terraform ignore rules
- Runtime defaults used during successful apply:
  - region: `us-west-1`
  - `db_engine_version`: `15.12` (stack default updated from `15.7`)
- Bootstrap implementation note (RDS compatibility):
  - replication user creation uses `LOGIN PASSWORD` and grants `rds_replication` when present
  - fallback `ALTER ROLE ... REPLICATION` is only used when `rds_replication` role does not exist
- Outputs are available for local integration test configuration (`PG*`, `AWS_REGION`, `KINESIS_STREAM`).

## Phase 2: Local Integration Tests (deliverable 2)

### Goals
- Add integration tests for `services/src/cdc_logical_replication` that run locally and target the Phase 1 infra.
- No CI/CD integration; tests are opt-in locally.

### Test Harness Contract (Phase 2)
Tests read config from env vars only (no secrets committed):

- Postgres: `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`
- AWS: `AWS_REGION`
- Kinesis: `KINESIS_STREAM`
- Optional override: `REPLICATION_SLOT` (default `etl_slot_wal2json`)

Tests assume AWS creds already available via env vars.

### Suggested Test Placement (Phase 2)
- `services/tests/integration/test_cdc_logical_replication_e2e.py` (and supporting fixtures)
- Mark with `pytest.mark.integration` and skip unless `RUN_INTEGRATION_TESTS=1` is set.

### Suggested Test Scenarios (Phase 2)
- Postgres setup:
  - Create a small test table.
  - Ensure replication slot exists via `ensure_replication_slot()`.
- Replication stream:
  - Insert/update/delete rows and confirm wal2json frames are produced (consume a bounded number of frames,
    timeouts).
- Kinesis publish:
  - Run publisher path that writes to Kinesis stream (bounded, deterministic batch sizes).
  - Read back from Kinesis (shard iterator) and assert payload shape and ordering constraints as appropriate.

### Phase 2 Acceptance Criteria
- `uv run pytest -q -m integration` (or equivalent) runs locally when env vars are set and infra exists.
- Tests are skipped by default unless explicitly enabled.

### Checkpoint (Execution Log, Not Plan Changes)
- Date: February 12, 2026
- Status: Phase 2 implemented and validated locally against the Phase 1 dev stack outputs.
- Test coverage added:
  - `services/tests/integration/test_cdc_logical_replication_e2e.py`
  - `services/tests/test_settings.py` (enforces `WAL2JSON_FORMAT_VERSION=2` only)
  - updated `services/tests/test_partition_key.py` for format-version 2 envelope semantics
- Runtime/implementation deltas required to complete Phase 2:
  - psycopg `3.3.2` `cursor.copy()` rejects `COPY_BOTH`; replication streaming now uses low-level `copy_from`/`copy_to` with explicit `COPY_BOTH` status checks in `services/src/cdc_logical_replication/replication.py`.
  - replication read loop changed to a persistent non-cancelled read task; timeout polling no longer cancels active `COPY_BOTH` reads.
  - wal2json `format-version=2` on RDS emits top-level action envelopes (`action`, `schema`, `table`, `columns`/`identity`, `pk`) instead of v1-style `change[]` batches; integration parsing/assertions and partition-key extraction were aligned to this shape.
  - integration test uses an isolated per-test replication slot (created + best-effort dropped) to avoid interference from stale shared-slot backlog.
- Validation summary:
  - `uv run pytest -q tests -m "not integration"` passed.
  - live integration test passed with elevated permissions using env vars sourced from Terraform outputs.

## Assumptions
- AWS account has default VPC (or implementer documents `vpc_id`/`subnet_ids` path).
- RDS supports `wal2json` plugin for the chosen engine version.
- Public dev RDS with IP allowlist is acceptable for this developer-only workflow.
