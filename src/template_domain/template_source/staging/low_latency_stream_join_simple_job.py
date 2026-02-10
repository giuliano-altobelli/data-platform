"""Simple low-latency streaming join example for tiny event volumes."""

from __future__ import annotations

import argparse
import json
import logging
from typing import Any

QUERY_NAME = "template_source_low_latency_simple"
MAX_TRIGGER_EXECUTION_MS = 45_000
MAX_BACKLOG_STREAK = 6

LOGGER = logging.getLogger(__name__)


def _spark_and_functions() -> tuple[Any, Any]:
    try:
        from pyspark.sql import SparkSession  # type: ignore[import-not-found]
        from pyspark.sql import functions as F  # type: ignore[import-not-found]
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "This module requires Databricks Runtime with pyspark installed."
        ) from exc

    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    return spark, F


def _to_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _to_int(value: Any) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _register_simple_monitoring_listener(spark: Any, query_name: str) -> Any:
    try:
        from pyspark.sql.streaming import StreamingQueryListener  # type: ignore[import-not-found]
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "This module requires Databricks Runtime with pyspark installed."
        ) from exc

    class _SimpleMonitoringListener(StreamingQueryListener):
        def __init__(self) -> None:
            super().__init__()
            self._backlog_streak = 0

        def onQueryStarted(self, event: Any) -> None:
            LOGGER.info(
                "STREAM_MONITOR %s",
                json.dumps(
                    {
                        "query_name": query_name,
                        "status": "started",
                        "id": str(getattr(event, "id", "")),
                        "run_id": str(getattr(event, "runId", "")),
                    },
                    sort_keys=True,
                ),
            )

        def onQueryProgress(self, event: Any) -> None:
            payload = json.loads(event.progress.json)
            if payload.get("name") != query_name:
                return

            sources = payload.get("sources", [])
            input_rows_per_second = sum(
                _to_float(source.get("inputRowsPerSecond")) for source in sources
            )
            processed_rows_per_second = _to_float(payload.get("processedRowsPerSecond"))
            trigger_execution_ms = _to_int(payload.get("durationMs", {}).get("triggerExecution"))
            state_operators = payload.get("stateOperators", [])
            state_rows_total = sum(
                _to_int(operator.get("numRowsTotal")) for operator in state_operators
            )

            is_backlog_batch = input_rows_per_second > processed_rows_per_second
            if is_backlog_batch:
                self._backlog_streak += 1
            else:
                self._backlog_streak = 0

            metrics = {
                "query_name": query_name,
                "batch_id": _to_int(payload.get("batchId")),
                "input_rows_per_second": round(input_rows_per_second, 3),
                "processed_rows_per_second": round(processed_rows_per_second, 3),
                "trigger_execution_ms": trigger_execution_ms,
                "state_rows_total": state_rows_total,
                "backlog_streak": self._backlog_streak,
            }
            LOGGER.info("STREAM_MONITOR %s", json.dumps(metrics, sort_keys=True))

            latency_breach = trigger_execution_ms > MAX_TRIGGER_EXECUTION_MS
            backlog_breach = self._backlog_streak >= MAX_BACKLOG_STREAK
            if latency_breach or backlog_breach:
                alert = dict(metrics)
                alert["latency_breach"] = latency_breach
                alert["backlog_breach"] = backlog_breach
                LOGGER.warning("STREAM_MONITOR_ALERT %s", json.dumps(alert, sort_keys=True))

        def onQueryTerminated(self, event: Any) -> None:
            LOGGER.info(
                "STREAM_MONITOR %s",
                json.dumps(
                    {
                        "query_name": query_name,
                        "status": "terminated",
                        "id": str(getattr(event, "id", "")),
                        "run_id": str(getattr(event, "runId", "")),
                        "exception": getattr(event, "exception", None),
                    },
                    sort_keys=True,
                ),
            )

    listener = _SimpleMonitoringListener()
    spark.streams.addListener(listener)
    return listener


def run_simple_low_latency_join(
    *,
    fact_stream_table: str,
    session_stream_table: str,
    attribution_stream_table: str,
    customer_dim_table: str,
    product_dim_table: str,
    target_table: str,
    checkpoint_location: str,
    trigger_interval: str = "5 seconds",
) -> Any:
    spark, F = _spark_and_functions()

    spark.conf.set("spark.sql.shuffle.partitions", "8")
    spark.conf.set(
        "spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled",
        "true",
    )

    fact = (
        spark.readStream.table(fact_stream_table)
        .withWatermark("event_ts", "30 seconds")
        .select("event_id", "event_ts", "user_id", "customer_id", "product_id", "metric_value")
    )
    sessions = (
        spark.readStream.table(session_stream_table)
        .withWatermark("session_ts", "30 seconds")
        .select("user_id", "session_id", "session_ts")
    )
    attributions = (
        spark.readStream.table(attribution_stream_table)
        .withWatermark("attribution_ts", "45 seconds")
        .select("event_id", "campaign_id", "attribution_ts")
    )

    core = (
        fact.alias("f")
        .join(
            sessions.alias("s"),
            F.expr(
                """
                f.user_id = s.user_id
                AND s.session_ts >= f.event_ts - interval 30 seconds
                AND s.session_ts <= f.event_ts + interval 30 seconds
                """
            ),
            "left",
        )
        .join(
            attributions.alias("a"),
            F.expr(
                """
                f.event_id = a.event_id
                AND a.attribution_ts >= f.event_ts
                AND a.attribution_ts <= f.event_ts + interval 45 seconds
                """
            ),
            "left",
        )
    )

    customer_dim = F.broadcast(
        spark.read.table(customer_dim_table).select("customer_id", "customer_segment")
    )
    product_dim = F.broadcast(
        spark.read.table(product_dim_table).select("product_id", "product_category")
    )

    result = (
        core.join(customer_dim, on="customer_id", how="left")
        .join(product_dim, on="product_id", how="left")
        .select(
            "event_id",
            "event_ts",
            "user_id",
            "customer_id",
            "product_id",
            "metric_value",
            "session_id",
            "campaign_id",
            "customer_segment",
            "product_category",
            F.current_timestamp().alias("processed_at"),
        )
    )

    return (
        result.writeStream.queryName(QUERY_NAME)
        .trigger(processingTime=trigger_interval)
        .option("checkpointLocation", checkpoint_location)
        .outputMode("append")
        .toTable(target_table)
    )


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="Run a simple low-latency streaming join example.")
    parser.add_argument("--fact-stream-table", required=True)
    parser.add_argument("--session-stream-table", required=True)
    parser.add_argument("--attribution-stream-table", required=True)
    parser.add_argument("--customer-dim-table", required=True)
    parser.add_argument("--product-dim-table", required=True)
    parser.add_argument("--target-table", required=True)
    parser.add_argument("--checkpoint-location", required=True)
    parser.add_argument("--trigger-interval", default="5 seconds")
    args = parser.parse_args(argv)

    spark, _ = _spark_and_functions()
    listener = _register_simple_monitoring_listener(spark, query_name=QUERY_NAME)
    query = None
    try:
        query = run_simple_low_latency_join(
            fact_stream_table=args.fact_stream_table,
            session_stream_table=args.session_stream_table,
            attribution_stream_table=args.attribution_stream_table,
            customer_dim_table=args.customer_dim_table,
            product_dim_table=args.product_dim_table,
            target_table=args.target_table,
            checkpoint_location=args.checkpoint_location,
            trigger_interval=args.trigger_interval,
        )
        query.awaitTermination()
    finally:
        try:
            spark.streams.removeListener(listener)
        except Exception:  # pragma: no cover
            LOGGER.exception("Failed to remove streaming query listener.")
        if query is not None:
            LOGGER.info("Stopped query '%s'.", QUERY_NAME)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
