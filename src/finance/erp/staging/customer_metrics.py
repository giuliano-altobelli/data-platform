"""Staging metrics table for the tightly-coupled finance/erp DLT pipeline."""

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

        def expect_or_drop(self, *args: object, **kwargs: object):
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
    name="finance_erp_staging_customer_metrics",
    comment="Business-level features derived from base ERP transactions.",
)
@dp.expect_or_drop("non_null_customer_id", "customer_id IS NOT NULL")
def finance_erp_staging_customer_metrics():
    spark, F = _spark_and_functions()

    return (
        spark.readStream.table("finance_erp_base_transactions")
        .withColumn("event_date", F.to_date("event_time"))
        .withColumn("is_high_value", F.col("amount") >= F.lit(1000.0))
    )
