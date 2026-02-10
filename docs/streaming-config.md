# Create Source-Cited Cluster Configuration Runbook for Low-Latency Streaming Job

## Summary
Add a new operational guide under `docs/` that explains the recommended fixed-size Databricks cluster configuration for the simple streaming join job, why each setting was chosen, and when to scale. The document will explicitly separate:
- Source-backed recommendations (with direct citations)
- Environment-specific constraints and inferred decisions (clearly labeled)

Chosen defaults:
- Location: new docs guide
- Depth: practical runbook

## Important Changes / Interfaces
1. New documentation file
- Path: `docs/low-latency-streaming-cluster-config.md`

2. README index update
- Add one bullet under existing docs links in `README.md` pointing to the new guide.

3. No code/runtime behavior changes
- No Spark job logic changes
- No spec/resource changes
- No API/type/schema changes

## Doc Content Specification (Decision Complete)
The new doc must contain these sections in order:

1. **Purpose and Scope**
- State that this runbook applies to `src/template_domain/template_source/staging/low_latency_stream_join_simple_job.py`.

2. **Recommended Baseline Cluster Configuration**
- Provide the exact YAML snippet already recommended:
  - `num_workers: 2`
  - fixed size (no autoscaling)
  - compute-optimized workers
  - `runtime_engine: STANDARD` (Photon disabled for this node constraint)
  - Spark conf keys used in this project:
    - `spark.sql.shuffle.partitions`
    - `spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled`
    - `spark.sql.streaming.noDataMicroBatches.enabled`
- Include a short “when to scale to 3–4 workers” rule.

3. **Why This Configuration**
- Bullet-by-bullet rationale mapped to each setting:
  - compute-optimized workers
  - shuffle partitions sizing heuristic
  - RocksDB + changelog checkpointing
  - `noDataMicroBatches` latency tradeoff
  - fixed-size cluster rationale for tiny throughput and stable stateful joins
- Mark any non-directly-documented recommendations as **Inference**.

4. **Photon Note**
- Add a short subsection:
  - “For this environment/node choice we run `runtime_engine: STANDARD`.”
  - “If switching to Photon-supported node families, reevaluate with `runtime_engine: PHOTON`.”
- Label this as environment constraint/inference if no authoritative instance-matrix source is cited.

5. **Operations Checklist**
- Pre-deploy checklist:
  - verify checkpoint path
  - verify notification channel
  - confirm fixed workers and spark conf
- Post-deploy checks:
  - where to view `inputRowsPerSecond`
  - compare `processedRowsPerSecond`
  - inspect trigger execution latency and state growth

6. **Tuning Playbook**
- Stepwise tuning:
  1. Keep workers fixed; increase from 2 -> 4 only if sustained backlog/latency breach.
  2. Keep `spark.sql.shuffle.partitions` aligned (8 -> 16 when scaling workers).
  3. Re-check latency and state metrics after each change.

7. **Source Mapping (Required)**
- Add a dedicated section with explicit claim-to-source mapping and URLs.

## Required Citations
Use these links in the doc and attach them to the relevant claims:

1. Databricks stateful streaming optimization  
`https://docs.databricks.com/aws/en/structured-streaming/stateful-streaming`  
Claims:
- compute-optimized worker recommendation
- shuffle partitions at ~1–2x cluster cores
- `spark.sql.streaming.noDataMicroBatches.enabled=false` tradeoff
- recommendation to use RocksDB + changelog checkpointing for stateful streams

2. Databricks RocksDB state store  
`https://docs.databricks.com/aws/en/structured-streaming/rocksdb-state-store`  
Claims:
- changelog checkpointing lowers checkpoint duration/end-to-end latency
- recommended for stateful workloads
- enablement config key and runtime note

3. Spark 4.1.1 DataStreamWriter trigger semantics  
`https://spark.apache.org/docs/4.1.1/api/java/org/apache/spark/sql/streaming/DataStreamWriter`  
and/or  
`https://spark.apache.org/docs/4.1.1/api/python/reference/pyspark.sql/api/pyspark.sql.streaming.DataStreamWriter.trigger.html`  
Claims:
- processing-time trigger behavior
- default trigger behavior (as fast as possible / ProcessingTime(0))

## Explicit Assumptions and Defaults
- This runbook targets tiny-volume, latency-critical stateful micro-batch streaming.
- Fixed-size `2` workers is baseline; autoscaling is intentionally disabled for stability (inference based on workload characteristics).
- Photon guidance is constrained by current environment/node compatibility statement from the team; no additional external support matrix will be asserted unless separately sourced.
- Alerting and runtime metrics remain as implemented in the existing simple monitoring setup.

## Test Cases / Validation Scenarios
1. **Doc quality checks**
- Run markdown lint/spell-check if available; otherwise verify formatting and link integrity manually.

2. **Source traceability**
- Verify each nontrivial recommendation sentence has a citation or is tagged as inference.

3. **Repo integration**
- Confirm README link resolves to `docs/low-latency-streaming-cluster-config.md`.

4. **Consistency check**
- Ensure YAML snippet in doc matches the currently recommended runtime settings and naming used in job/spec artifacts.
