# Real-Time E-Commerce Data Platform with RAG

Capstone project for the SDAIA Academy program **"Modern Data Engineering for AI Systems"**.

An end-to-end data pipeline that ingests real e-commerce transactions through Kafka,
validates them against a data contract, lands them in a Bronze/Silver/Gold Delta Lakehouse,
and serves a Retrieval-Augmented Generation (RAG) assistant over the Gold layer —
all orchestrated by an Airflow DAG with Great Expectations quality gates and
OpenLineage lineage events.

> **Status: work in progress** — built incrementally; see the commit history.

## Architecture

```
producer.py ──► Kafka topic: transactions ──► consumer.py + Pydantic contract
                                   │ valid                     │ malformed
                                   ▼                           ▼
                            Bronze (Delta)            dead-letter topic (+ rejection reason)
                                   │ dedupe → date parsing → grain aggregation → MERGE
                                   ▼
                            Silver (Delta)  ◄── schema enforcement (bad write refused)
                                   │ genuine aggregate (GROUP BY), rank-annotated
                                   ▼
                            Gold (Delta) ──► product documents ──► ChromaDB + BM25
                                                    ──► RRF fusion ──► cross-encoder rerank
                                                    ──► answers with citations

Airflow DAG:  produce → consume/validate → GE gate (bronze) → bronze→silver
              → GE gate (silver) → silver→gold → build RAG index
              (a failed quality gate halts all downstream stages)
OpenLineage:  START / COMPLETE / FAIL events emitted per stage
```

## Dataset

[UCI Online Retail](https://archive.ics.uci.edu/dataset/352/online+retail) — 541,909 real
transactions from a UK online retailer (2010–2011), with genuine quality problems
(25% missing CustomerID, negative quantities, duplicates) that exercise the failure paths.

Citation (CC BY 4.0): Chen, D. (2015). *Online Retail* [Dataset]. UCI Machine Learning
Repository. https://doi.org/10.24432/C5BW33

## How to run

Prerequisites: Docker Desktop.

```bash
docker compose up --build
```

- Airflow UI: http://localhost:8080 (user `admin` / password `admin`)
- Kafka broker: `kafka:9092` inside the compose network, `localhost:29092` from the host

Detailed run steps, expected output, and evidence of executed runs: see
[`evidence/`](evidence/) and `docs/ARCHITECTURE.md` (added as the build progresses).

## Repository layout

| Path | Purpose |
|---|---|
| `docker/` | Airflow image: JDK 17 + isolated compute venv (Spark, Delta, RAG, GE) |
| `dags/` | Airflow DAG wiring all pipeline stages |
| `src/` | Pipeline stages: producer, consumer/contract, bronze→silver→gold |
| `src/rag/` | Chunking, indexing (ChromaDB + BM25), hybrid search, reranking |
| `evidence/` | Captured outputs of real runs — happy path AND failure paths |
| `docs/` | Architecture and technical documentation |

## Training program attribution

This project was completed as the capstone for **"Modern Data Engineering for AI Systems"**,
a 5-day training program by **SDAIA Academy** (delivered via Learning Space),
trainer Mohammed Albeladi, session dates 2–6 August 2026.

SDAIA Academy on GitHub: https://github.com/SDAIAAcademy
