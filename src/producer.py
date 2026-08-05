"""Kafka producer — replays UCI retail rows as live transaction events.

Batches (for the MERGE update+insert evidence in one run):
  A: rows 0..4999                      -> initial load
  B: rows 0..1999 with doubled Quantity (updates) + rows 5000..6999 (inserts)

--inject-malformed N appends N deliberately broken messages so the consumer's
contract gate provably routes them to the dead-letter topic with a reason.
"""
import argparse
import csv
import json

from kafka import KafkaProducer

from config import KAFKA_BOOTSTRAP, RAW_CSV, TOPIC_TRANSACTIONS
from lineage import stage

MALFORMED = [
    {"InvoiceNo": "FREE-STUFF", "StockCode": "85123A", "Description": "bad invoice format",
     "Quantity": "1", "InvoiceDate": "12/1/2010 8:26", "UnitPrice": "2.55",
     "CustomerID": "17850", "Country": "United Kingdom"},
    {"InvoiceNo": "536365", "StockCode": "85123A", "Description": "unparseable date",
     "Quantity": "1", "InvoiceDate": "not-a-date", "UnitPrice": "2.55",
     "CustomerID": "17850", "Country": "United Kingdom"},
    {"InvoiceNo": "536365", "StockCode": "85123A", "Description": "quantity is text",
     "Quantity": "many", "InvoiceDate": "12/1/2010 8:26", "UnitPrice": "2.55",
     "CustomerID": "17850", "Country": "United Kingdom"},
]


def load_rows(limit: int) -> list[dict]:
    with open(RAW_CSV, encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        rows = []
        for row in reader:
            rows.append(row)
            if len(rows) >= limit:
                break
    return rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch", choices=["A", "B"], default="A")
    parser.add_argument("--inject-malformed", type=int, default=0)
    args = parser.parse_args()

    rows = load_rows(7000)
    if args.batch == "A":
        events = rows[:5000]
    else:
        updates = []
        for row in rows[:2000]:
            row = dict(row)
            try:
                row["Quantity"] = str(int(row["Quantity"]) * 2)
            except ValueError:
                pass
            updates.append(row)
        events = updates + rows[5000:7000]

    events = events + MALFORMED[: args.inject_malformed]

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
    )
    for event in events:
        producer.send(TOPIC_TRANSACTIONS, event)
    producer.flush()
    producer.close()
    print(f"[PRODUCER] batch {args.batch}: sent {len(events)} events "
          f"({args.inject_malformed} malformed) to '{TOPIC_TRANSACTIONS}' @ {KAFKA_BOOTSTRAP}")


if __name__ == "__main__":
    with stage("produce_to_kafka"):
        main()
