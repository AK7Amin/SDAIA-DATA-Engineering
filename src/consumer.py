"""Kafka consumer with the contract gate (rubric deliverable 1).

Valid records   -> data/landing/<run>.jsonl  (picked up by the bronze Spark job)
Malformed ones  -> dead-letter topic, with the rejection reason recorded in
                   the message itself — nothing is silently dropped.

Terminates after IDLE_POLLS consecutive empty polls, so the Airflow task that
wraps it actually finishes (a bare Kafka consumer loops forever).
"""
import json
import os
import time

from kafka import KafkaConsumer, KafkaProducer
from pydantic import ValidationError

from config import KAFKA_BOOTSTRAP, LANDING_DIR, TOPIC_DLQ, TOPIC_TRANSACTIONS
from contracts import RetailTransaction
from lineage import stage

IDLE_POLLS = 3
POLL_TIMEOUT_MS = 3000


def main() -> None:
    consumer = KafkaConsumer(
        TOPIC_TRANSACTIONS,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="capstone-ingest",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    dlq = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    os.makedirs(LANDING_DIR, exist_ok=True)
    out_path = os.path.join(LANDING_DIR, f"validated_{int(time.time())}.jsonl")

    accepted = rejected = idle = 0
    with open(out_path, "w", encoding="utf-8") as out:
        while idle < IDLE_POLLS:
            batches = consumer.poll(timeout_ms=POLL_TIMEOUT_MS)
            if not batches:
                idle += 1
                continue
            idle = 0
            for records in batches.values():
                for record in records:
                    try:
                        tx = RetailTransaction(**record.value)
                    except (ValidationError, TypeError) as exc:
                        reason = str(exc).splitlines()[0:2]
                        dlq.send(TOPIC_DLQ, {
                            "rejected_record": record.value,
                            "rejection_reason": " | ".join(reason),
                            "source_offset": record.offset,
                            "source_partition": record.partition,
                        })
                        rejected += 1
                        print(f"[CONSUMER] REJECTED @offset {record.offset}: {reason[0]}")
                        continue
                    row = tx.model_dump()
                    row["_kafka_offset"] = record.offset
                    row["_kafka_partition"] = record.partition
                    out.write(json.dumps(row) + "\n")
                    accepted += 1

    dlq.flush()
    dlq.close()
    consumer.close()
    print(f"[CONSUMER] done: {accepted} accepted -> {out_path}")
    print(f"[CONSUMER]       {rejected} rejected -> dead-letter topic '{TOPIC_DLQ}'")


if __name__ == "__main__":
    with stage("consume_validate_to_landing"):
        main()
