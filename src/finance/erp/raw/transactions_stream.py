"""Raw streaming ingestion table for the tightly-coupled finance/erp DLT pipeline."""

try:
    from pyspark import pipelines as dp  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover

    class _DPStub:
        def table(self, *args: object, **kwargs: object):
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
    name="finance_erp_raw_transactions",
    comment="Raw ERP transactions ingested with Auto Loader.",
)
def finance_erp_raw_transactions():
    spark, F = _spark_and_functions()

    input_path = spark.conf.get("finance_erp_input_path")
    schema_location = spark.conf.get("finance_erp_schema_location")

    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", schema_location)
        .load(input_path)
        .withColumn("ingested_at", F.current_timestamp())
    )
