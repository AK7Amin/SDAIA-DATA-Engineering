"""Builds RAG source documents from the gold Delta table (rubric: RAG pipeline).

Reads the product-level aggregates written by the gold layer and renders one
multi-sentence English document per product to DATA_DIR/rag_docs.jsonl. The
documents are deliberately multi-sentence (4-6 sentences) so build_index.py's
sentence chunker has something real to split — a one-sentence document would
never demonstrate chunking at all.
"""
import json
import os
import sys
from pathlib import Path

# src/rag/ is one level below src/ on both the container (/opt/capstone/src)
# and the host (src/) layouts, so config.py is always the parent directory.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import DATA_DIR, GOLD_PATH  # noqa: E402
from lineage import stage  # noqa: E402

MAX_PRODUCTS = 1000
OUT_PATH = os.path.join(DATA_DIR, "rag_docs.jsonl")


def build_spark():
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("docs_from_gold")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def row_to_document(row):
    """Renders one gold row as a 4-6 sentence English document."""
    name = row["Description"] or row["StockCode"]
    revenue = float(row["total_revenue"] or 0.0)
    quantity = int(row["total_quantity"] or 0)
    invoices = int(row["num_invoices"] or 0)
    rank = int(row["revenue_rank"])
    total_products = int(row["total_products"])
    sentences = [
        f"{name} (product code {row['StockCode']}) is a product sold through the "
        f"online retail catalog.",
        f"It generated total revenue of {revenue:.2f} over the observed period.",
        f"A total of {quantity} units of this product were sold.",
        f"It appeared across {invoices} separate invoices.",
        f"Its top countries by sales were {row['top_countries']}.",
        f"It ranks {rank} out of {total_products} products by total revenue.",
    ]
    # numbers barely move an embedding ("ranks 1" ≈ "ranks 47"), words do —
    # superlative queries like "top product by revenue" need these spelled out
    if rank == 1:
        sentences.append(
            "It is the top product by revenue: the number one best-selling, "
            "highest-revenue product in the entire catalog."
        )
    elif rank <= 10:
        sentences.append(
            f"It is one of the top 10 best-selling products by revenue "
            f"(number {rank})."
        )
    return " ".join(sentences)


def main():
    spark = build_spark()
    df = spark.read.format("delta").load(GOLD_PATH)

    total = df.count()
    if total > MAX_PRODUCTS:
        print(
            f"[docs_from_gold] gold has {total} products, capping to top "
            f"{MAX_PRODUCTS} by total_revenue"
        )
        df = df.orderBy(df.total_revenue.desc()).limit(MAX_PRODUCTS)
    else:
        print(f"[docs_from_gold] gold has {total} products, keeping all")

    rows = df.collect()
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        for row in rows:
            doc = {"doc_id": row["StockCode"],
                   "title": (row["Description"] or row["StockCode"]).strip(),
                   "text": row_to_document(row)}
            f.write(json.dumps(doc) + "\n")

    print(f"[docs_from_gold] wrote {len(rows)} documents -> {OUT_PATH}")
    spark.stop()


if __name__ == "__main__":
    with stage("build_rag_documents"):
        main()
