"""Final serving table for the tightly-coupled finance/erp DLT pipeline."""

try:
    from pyspark import pipelines as dp  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover

    class _DPStub:
        def table(self, *args: object, **kwargs: object):
            def _decorator(func):
                return func

            return _decorator

        def expect(self, *args: object, **kwargs: object):
            def _decorator(func):
                return func

            return _decorator

    dp = _DPStub()


def _spark_and_functions():
    try:
        from pyspark.sql import SparkSession  # type: ignore[import-not-found]
        from pyspark.sql import functions as F  # type: ignore[import-not-found]
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "This module requires Databricks Runtime with pyspark installed."
        ) from exc

    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    return spark, F


@dp.table(
    name="finance_erp_final_customer_daily_summary",
    comment="Serving-ready customer/day summary built from tightly-coupled staging tables.",
)
@dp.expect("event_date_present", "event_date IS NOT NULL")
def finance_erp_final_customer_daily_summary():
    spark, F = _spark_and_functions()

    return (
        spark.readStream.table("finance_erp_staging_customer_metrics")
        .groupBy("customer_id", "event_date")
        .agg(
            F.count("*").alias("transaction_count"),
            F.sum("amount").alias("total_amount"),
            F.max("event_time").alias("last_transaction_at"),
            F.max("is_high_value").alias("has_high_value_txn"),
        )
    )
