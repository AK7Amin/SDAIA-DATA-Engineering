"""Deletes demo-injected rows (`_injected = true`) from silver, restoring a
clean state after the inject_corruption.py failure demo so the next pipeline
run passes the silver ge_gate again.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import SILVER_PATH  # noqa: E402
from lineage import stage  # noqa: E402

from delta import configure_spark_with_delta_pip  # noqa: E402
from delta.tables import DeltaTable  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("reset_demo")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def main() -> None:
    with stage("reset_demo"):
        spark = build_spark()
        try:
            before_df = spark.read.format("delta").load(SILVER_PATH)
            before_total = before_df.count()
            before_injected = before_df.filter("_injected = true").count()
            print(f"[RESET] silver rows before: {before_total:,} total, {before_injected:,} injected")

            delta_table = DeltaTable.forPath(spark, SILVER_PATH)
            delta_table.delete("_injected = true")

            after_total = spark.read.format("delta").load(SILVER_PATH).count()
            print(f"[RESET] silver rows after:  {after_total:,} total")
            print(f"[RESET] removed {before_total - after_total:,} injected row(s) -> clean state restored")
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
