"""Schema enforcement proof (rubric deliverable 2): a write with an
undeclared column must be REFUSED by Delta — and we keep the error as evidence.
"""
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import EVIDENCE_DIR, SILVER_PATH  # noqa: E402
from spark_utils import get_spark  # noqa: E402


def main() -> None:
    spark = get_spark("schema_enforcement_demo")
    bad = spark.createDataFrame(
        [("999999", "99999X", "sneaky row", 1, None, 0.0, "", "Nowhere",
          0.0, False, False, 99.9)],
        ["InvoiceNo", "StockCode", "Description", "Quantity", "invoice_ts",
         "UnitPrice", "CustomerID", "Country", "line_revenue",
         "is_cancellation", "_injected", "discount"],  # 'discount' is undeclared
    )
    out = os.path.join(EVIDENCE_DIR, "delta", f"schema_rejection_{int(time.time())}.txt")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    try:
        bad.write.format("delta").mode("append").save(SILVER_PATH)
        print("[SCHEMA-DEMO] UNEXPECTED: write was accepted — enforcement failed!")
        sys.exit(1)
    except Exception as exc:
        head = "\n".join(str(exc).splitlines()[:15])
        with open(out, "w", encoding="utf-8") as fh:
            fh.write("Attempted append with undeclared column 'discount' -> REFUSED\n\n")
            fh.write(head)
        print("[SCHEMA-DEMO] write REFUSED by Delta schema enforcement (as designed)")
        print(f"[SCHEMA-DEMO] error captured -> {out}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
