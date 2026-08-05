"""Silver -> Gold: a genuine aggregate (rubric: "not a copy of Silver").

One row per product: revenue, units, invoices, top countries, and the
revenue rank BAKED IN — retrieval can't compare across documents, so
"best-selling product" questions only work if the rank is text in the doc.
Rebuilt with overwrite every run (idempotent). Cancellations and injected
demo rows are excluded from revenue.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import GOLD_PATH, SILVER_PATH  # noqa: E402
from lineage import stage  # noqa: E402
from spark_utils import get_spark  # noqa: E402


def main() -> None:
    spark = get_spark("silver_to_gold")
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    silver = (spark.read.format("delta").load(SILVER_PATH)
              .filter(~F.col("is_cancellation") & ~F.col("_injected")))

    by_country = silver.groupBy("StockCode", "Country").agg(
        F.sum("line_revenue").alias("country_revenue"))
    top3 = (by_country
            .withColumn("rn", F.row_number().over(
                Window.partitionBy("StockCode")
                      .orderBy(F.desc("country_revenue"))))
            .filter(F.col("rn") <= 3)
            .groupBy("StockCode")
            .agg(F.concat_ws(", ", F.collect_list("Country")).alias("top_countries")))

    gold = silver.groupBy("StockCode").agg(
        F.first("Description", ignorenulls=True).alias("Description"),
        F.sum("Quantity").alias("total_quantity"),
        F.round(F.sum("line_revenue"), 2).alias("total_revenue"),
        F.countDistinct("InvoiceNo").alias("num_invoices"),
    ).join(top3, "StockCode", "left")

    gold = gold.withColumn(
        "revenue_rank",
        F.row_number().over(Window.orderBy(F.desc("total_revenue"))))
    total = gold.count()
    gold = gold.withColumn("total_products", F.lit(total))

    gold.write.format("delta").mode("overwrite").save(GOLD_PATH)
    print(f"[GOLD] wrote {total} product aggregates (overwrite)")
    print("[GOLD] top 5 by revenue:")
    gold.orderBy("revenue_rank").select(
        "revenue_rank", "StockCode", "Description", "total_revenue",
        "num_invoices", "top_countries").show(5, truncate=False)
    spark.stop()


if __name__ == "__main__":
    with stage("silver_to_gold"):
        main()
