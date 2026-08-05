"""Demo-only script: appends 5 duplicated silver rows with `_injected=True`
so the silver ge_gate's ExpectCompoundColumnsToBeUnique(InvoiceNo, StockCode)
expectation has a real defect to catch.

Never run this against a production silver table — it exists purely to
demonstrate that the quality gate actually blocks the pipeline.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import SILVER_PATH  # noqa: E402
from lineage import stage  # noqa: E402

from delta import configure_spark_with_delta_pip  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.functions import lit  # noqa: E402

ROWS_TO_INJECT = 5


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("inject_corruption")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def main() -> None:
    with stage("inject_corruption"):
        spark = build_spark()
        try:
            silver = spark.read.format("delta").load(SILVER_PATH)
            original_columns = silver.columns  # preserve exact column order for append
            sample = silver.limit(ROWS_TO_INJECT)

            # Re-using the sampled rows' own InvoiceNo/StockCode is the point:
            # appending them back creates exact-duplicate (InvoiceNo,
            # StockCode) keys, which is the defect the silver gate's
            # compound-uniqueness expectation is designed to catch.
            corrupted = (
                sample.drop("_injected")
                .withColumn("_injected", lit(True))
                .select(*original_columns)
            )

            injected_keys = [
                (r["InvoiceNo"], r["StockCode"])
                for r in corrupted.select("InvoiceNo", "StockCode").collect()
            ]
            print(f"[INJECT] duplicating {len(injected_keys)} row(s) into silver:")
            for invoice_no, stock_code in injected_keys:
                print(f"  InvoiceNo={invoice_no} StockCode={stock_code}")

            corrupted.write.format("delta").mode("append").save(SILVER_PATH)
            print(f"[INJECT] appended {len(injected_keys)} corrupted row(s) -> {SILVER_PATH}")
            print("[INJECT] next `python ge_gate.py --layer silver` run should now FAIL.")
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
