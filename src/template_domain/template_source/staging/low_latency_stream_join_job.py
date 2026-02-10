"""Low-latency Spark Structured Streaming join job for tiny-throughput workloads."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class LowLatencyJoinSettings:
    """Runtime settings for the low-latency five-table enrichment query."""

    fact_stream_table: str
    session_stream_table: str
    attribution_stream_table: str
    customer_dim_table: str
    product_dim_table: str
    target_table: str
    checkpoint_location: str
    trigger_interval: str = "5 seconds"
    fact_watermark: str = "30 seconds"
    session_watermark: str = "30 seconds"
    attribution_watermark: str = "45 seconds"
    session_window_seconds: int = 30
    attribution_window_seconds: int = 45
    shuffle_partitions: int = 8
    no_data_micro_batches: bool = False


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


def _delta_table() -> Any:
    try:
        from delta.tables import DeltaTable  # type: ignore[import-not-found]
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "This module requires delta-spark support to run foreachBatch merge."
        ) from exc
    return DeltaTable


def _bool_to_conf(value: bool) -> str:
    return "true" if value else "false"


def _parse_bool(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y"}:
        return True
    if normalized in {"0", "false", "no", "n"}:
        return False
    raise ValueError(f"Unsupported boolean literal: {value!r}")


def _apply_runtime_tuning(spark: Any, settings: LowLatencyJoinSettings) -> None:
    spark.conf.set("spark.sql.shuffle.partitions", str(settings.shuffle_partitions))
    spark.conf.set(
        "spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled",
        "true",
    )
    spark.conf.set(
        "spark.sql.streaming.noDataMicroBatches.enabled",
        _bool_to_conf(settings.no_data_micro_batches),
    )


def _build_enriched_stream(spark: Any, F: Any, settings: LowLatencyJoinSettings) -> Any:
    fact = (
        spark.readStream.table(settings.fact_stream_table)
        .withWatermark("event_ts", settings.fact_watermark)
        .select(
            "event_id",
            "event_ts",
            "user_id",
            "customer_id",
            "product_id",
            "metric_value",
        )
    )
    sessions = (
        spark.readStream.table(settings.session_stream_table)
        .withWatermark("session_ts", settings.session_watermark)
        .select("user_id", "session_id", "session_ts", "session_channel")
    )
    attributions = (
        spark.readStream.table(settings.attribution_stream_table)
        .withWatermark("attribution_ts", settings.attribution_watermark)
        .select("event_id", "campaign_id", "attribution_ts")
    )

    fact_session = fact.alias("f").join(
        sessions.alias("s"),
        F.expr(
            f"""
            f.user_id = s.user_id
            AND s.session_ts >= f.event_ts - interval {settings.session_window_seconds} seconds
            AND s.session_ts <= f.event_ts + interval {settings.session_window_seconds} seconds
            """
        ),
        "left",
    )

    core = fact_session.alias("fs").join(
        attributions.alias("a"),
        F.expr(
            f"""
            fs.event_id = a.event_id
            AND a.attribution_ts >= fs.event_ts
            AND a.attribution_ts <= fs.event_ts + interval {settings.attribution_window_seconds} seconds
            """
        ),
        "left",
    )

    customer_dim = F.broadcast(
        spark.read.table(settings.customer_dim_table).select(
            "customer_id",
            "customer_segment",
            "customer_status",
        )
    )
    product_dim = F.broadcast(
        spark.read.table(settings.product_dim_table).select(
            "product_id",
            "product_category",
            "product_family",
        )
    )

    return (
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
            "session_ts",
            "session_channel",
            "campaign_id",
            "attribution_ts",
            "customer_segment",
            "customer_status",
            "product_category",
            "product_family",
            F.current_timestamp().alias("processed_at"),
        )
    )


def _build_upsert_writer(spark: Any, settings: LowLatencyJoinSettings):
    delta_table = _delta_table()

    def _upsert_batch(batch_df: Any, _batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return

        if spark.catalog.tableExists(settings.target_table):
            target = delta_table.forName(spark, settings.target_table)
            (
                target.alias("t")
                .merge(batch_df.alias("s"), "t.event_id = s.event_id")
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
            return

        batch_df.write.format("delta").mode("overwrite").saveAsTable(settings.target_table)

    return _upsert_batch


def start_low_latency_join(settings: LowLatencyJoinSettings) -> Any:
    spark, F = _spark_and_functions()
    _apply_runtime_tuning(spark, settings)
    enriched_stream = _build_enriched_stream(spark, F, settings)

    return (
        enriched_stream.writeStream.queryName("template_source_low_latency_stream_join")
        .trigger(processingTime=settings.trigger_interval)
        .option("checkpointLocation", settings.checkpoint_location)
        .foreachBatch(_build_upsert_writer(spark, settings))
        .start()
    )


def _argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a low-latency five-table streaming enrichment query."
    )
    parser.add_argument("--fact-stream-table", required=True)
    parser.add_argument("--session-stream-table", required=True)
    parser.add_argument("--attribution-stream-table", required=True)
    parser.add_argument("--customer-dim-table", required=True)
    parser.add_argument("--product-dim-table", required=True)
    parser.add_argument("--target-table", required=True)
    parser.add_argument("--checkpoint-location", required=True)
    parser.add_argument("--trigger-interval", default="5 seconds")
    parser.add_argument("--fact-watermark", default="30 seconds")
    parser.add_argument("--session-watermark", default="30 seconds")
    parser.add_argument("--attribution-watermark", default="45 seconds")
    parser.add_argument("--session-window-seconds", type=int, default=30)
    parser.add_argument("--attribution-window-seconds", type=int, default=45)
    parser.add_argument("--shuffle-partitions", type=int, default=8)
    parser.add_argument("--no-data-micro-batches", default="false")
    parser.add_argument("--await-termination", default="true")
    return parser


def _settings_from_args(args: argparse.Namespace) -> LowLatencyJoinSettings:
    return LowLatencyJoinSettings(
        fact_stream_table=args.fact_stream_table,
        session_stream_table=args.session_stream_table,
        attribution_stream_table=args.attribution_stream_table,
        customer_dim_table=args.customer_dim_table,
        product_dim_table=args.product_dim_table,
        target_table=args.target_table,
        checkpoint_location=args.checkpoint_location,
        trigger_interval=args.trigger_interval,
        fact_watermark=args.fact_watermark,
        session_watermark=args.session_watermark,
        attribution_watermark=args.attribution_watermark,
        session_window_seconds=args.session_window_seconds,
        attribution_window_seconds=args.attribution_window_seconds,
        shuffle_partitions=args.shuffle_partitions,
        no_data_micro_batches=_parse_bool(args.no_data_micro_batches),
    )


def main(argv: list[str] | None = None) -> int:
    parser = _argument_parser()
    args = parser.parse_args(argv)
    settings = _settings_from_args(args)
    query = start_low_latency_join(settings)
    if _parse_bool(args.await_termination):
        query.awaitTermination()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
