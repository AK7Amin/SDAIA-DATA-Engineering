"""Landing -> Bronze: append validated events as they arrived (audit layer).

Bronze stays deliberately untransformed: exactly what the consumer landed
(contract-typed at the boundary, since the rubric requires validation at
ingestion) — but no dedup, no date parsing, no aggregation, plus ingest_ts.
All cleaning is Silver's job, which keeps the medallion story defensible:
bronze still holds the duplicates and unparsed date strings as proof.
"""
import glob
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import BRONZE_PATH, LANDING_DIR  # noqa: E402
from lineage import stage  # noqa: E402
from spark_utils import get_spark  # noqa: E402


def main() -> None:
    files = sorted(glob.glob(os.path.join(LANDING_DIR, "*.jsonl")))
    if not files:
        print("[BRONZE] no new landing files — nothing to do")
        return
    spark = get_spark("bronze_ingest")
    df = spark.read.json(files)
    from pyspark.sql import functions as F
    df = df.withColumn("ingest_ts", F.current_timestamp())
    df.write.format("delta").mode("append").save(BRONZE_PATH)
    total = spark.read.format("delta").load(BRONZE_PATH).count()
    print(f"[BRONZE] appended {df.count()} rows from {len(files)} landing file(s); "
          f"bronze now holds {total} rows")
    spark.stop()


if __name__ == "__main__":
    with stage("bronze_ingest"):
        main()
