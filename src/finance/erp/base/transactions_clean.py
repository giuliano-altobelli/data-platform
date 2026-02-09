"""Base-quality transaction table for the tightly-coupled finance/erp DLT pipeline."""

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
    name="finance_erp_base_transactions",
    comment="Base transaction normalization with explicit quality expectations.",
)
@dp.expect_or_drop("non_null_transaction_id", "transaction_id IS NOT NULL")
@dp.expect("non_negative_amount", "amount >= 0")
def finance_erp_base_transactions():
    spark, F = _spark_and_functions()

    return (
        spark.readStream.table("finance_erp_raw_transactions")
        .withColumn("transaction_id", F.col("transaction_id").cast("string"))
        .withColumn("customer_id", F.col("customer_id").cast("string"))
        .withColumn("amount", F.col("amount").cast("double"))
        .withColumn(
            "event_time",
            F.coalesce(F.col("event_time").cast("timestamp"), F.col("ingested_at")),
        )
    )
