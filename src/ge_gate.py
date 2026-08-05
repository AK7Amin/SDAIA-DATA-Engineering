"""CLI quality gate: `python ge_gate.py --layer bronze|silver`.

Reads the given Delta layer with pyspark+delta, hands it to pandas, and runs
a REAL Great Expectations 1.x fluent checkpoint (not a hand-rolled check) —
the checkpoint's `success` flag is what the Airflow task's exit code reports.

bronze suite: structural checks only (raw layer legitimately has duplicate
(InvoiceNo, StockCode) rows — e.g. re-delivered Kafka messages — so no
uniqueness expectation belongs here).

silver suite: ExpectCompoundColumnsToBeUnique(InvoiceNo, StockCode) is THE
gate that trips when inject_corruption.py appends duplicate keys.
"""
import argparse
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import BRONZE_PATH, EVIDENCE_DIR, SILVER_PATH  # noqa: E402
from lineage import stage  # noqa: E402

import great_expectations as gx  # noqa: E402
import great_expectations.expectations as gxe  # noqa: E402
from delta import configure_spark_with_delta_pip  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

LAYER_PATHS = {"bronze": BRONZE_PATH, "silver": SILVER_PATH}


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("ge_gate")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def build_suite(layer: str) -> gx.ExpectationSuite:
    suite = gx.ExpectationSuite(name=f"{layer}_quality_suite")
    if layer == "bronze":
        suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="InvoiceNo"))
        suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="StockCode"))
        suite.add_expectation(
            gxe.ExpectColumnValuesToMatchRegex(column="InvoiceNo", regex=r"^[A-Za-z]?\d{5,6}$")
        )
    else:  # silver
        suite.add_expectation(
            gxe.ExpectCompoundColumnsToBeUnique(column_list=["InvoiceNo", "StockCode"])
        )
        suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="invoice_ts"))
    return suite


def run_checkpoint(layer: str, df) -> tuple:
    """Runs a real GX 1.x fluent checkpoint. Returns (success, result)."""
    context = gx.get_context(mode="ephemeral")
    data_source = context.data_sources.add_pandas(f"pandas_{layer}")
    data_asset = data_source.add_dataframe_asset(name=f"{layer}_table")
    batch_definition = data_asset.add_batch_definition_whole_dataframe("whole_df")

    suite = context.suites.add(build_suite(layer))
    validation_definition = context.validation_definitions.add(
        gx.ValidationDefinition(name=f"{layer}_validation", data=batch_definition, suite=suite)
    )
    checkpoint = context.checkpoints.add(
        gx.Checkpoint(name=f"{layer}_checkpoint", validation_definitions=[validation_definition])
    )
    result = checkpoint.run(batch_parameters={"dataframe": df})
    return result.success, result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--layer", choices=["bronze", "silver"], required=True)
    args = parser.parse_args()
    layer = args.layer

    with stage(f"ge_gate_{layer}"):
        spark = build_spark()
        try:
            sdf = spark.read.format("delta").load(LAYER_PATHS[layer])
            df = sdf.toPandas()
            success, result = run_checkpoint(layer, df)
        finally:
            spark.stop()

        print(f"[GE GATE] layer={layer} rows={len(df)} success={success}")
        for run_result in result.run_results.values():
            for r in run_result["results"]:
                status = "PASSED" if r["success"] else "FAILED"
                print(f"  [{status}] {r['expectation_config']['type']}")

        os.makedirs(os.path.join(EVIDENCE_DIR, "ge_lineage"), exist_ok=True)
        timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
        verdict = "pass" if success else "fail"
        out_path = os.path.join(
            EVIDENCE_DIR, "ge_lineage", f"checkpoint_{layer}_{verdict}_{timestamp}.json"
        )
        with open(out_path, "w", encoding="utf-8") as fh:
            json.dump(result.describe_dict(), fh, indent=2, default=str)
        print(f"[GE GATE] full result -> {out_path}")

        if not success:
            # sys.exit inside the `with stage(...)` block: the SystemExit
            # propagates through stage()'s except clause, which emits a FAIL
            # lineage event and re-raises — Airflow then sees a failed task.
            sys.exit(1)


if __name__ == "__main__":
    main()
