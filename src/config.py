"""Shared configuration — resolves paths/endpoints for both host and container runs."""
import os

# On the host we talk to the broker's published port; inside the compose
# network the airflow container gets KAFKA_BOOTSTRAP=kafka:9092 from compose.
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")

TOPIC_TRANSACTIONS = "retail_transactions"
TOPIC_DLQ = "retail_transactions_dlq"

CAPSTONE_HOME = os.environ.get(
    "CAPSTONE_HOME", os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
DATA_DIR = os.path.join(CAPSTONE_HOME, "data")
EVIDENCE_DIR = os.path.join(CAPSTONE_HOME, "evidence")

RAW_ZIP = os.path.join(DATA_DIR, "online_retail.zip")
RAW_CSV = os.path.join(DATA_DIR, "online_retail.csv")
LANDING_DIR = os.path.join(DATA_DIR, "landing")        # validated JSONL from consumer
LAKE_DIR = os.path.join(DATA_DIR, "lake")
BRONZE_PATH = os.path.join(LAKE_DIR, "bronze")
SILVER_PATH = os.path.join(LAKE_DIR, "silver")
GOLD_PATH = os.path.join(LAKE_DIR, "gold")
CHROMA_DIR = os.path.join(DATA_DIR, "chroma")

DATE_FORMAT = "%m/%d/%Y %H:%M"  # UCI format: 12/1/2010 8:26 (no leading zeros)
