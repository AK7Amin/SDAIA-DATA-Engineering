"""Dump the dead-letter topic to a JSONL file — the rubric's proof that
malformed records were routed with their rejection reason recorded."""
import json
import os

from kafka import KafkaConsumer

from config import EVIDENCE_DIR, KAFKA_BOOTSTRAP, TOPIC_DLQ


def main() -> None:
    consumer = KafkaConsumer(
        TOPIC_DLQ,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        consumer_timeout_ms=5000,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    out_path = os.path.join(EVIDENCE_DIR, "ingestion", "dlq_contents.jsonl")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    count = 0
    with open(out_path, "w", encoding="utf-8") as out:
        for record in consumer:
            out.write(json.dumps(record.value, ensure_ascii=False) + "\n")
            count += 1
            print(f"[DLQ] offset {record.offset}: {record.value['rejection_reason']}")
    consumer.close()
    print(f"[DLQ] {count} dead-lettered messages -> {out_path}")


if __name__ == "__main__":
    main()
