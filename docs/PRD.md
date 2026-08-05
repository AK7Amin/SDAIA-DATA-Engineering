# Product Requirements Document

**Project:** Real-Time E-Commerce Data Platform with RAG
**Program:** SDAIA Academy — "Modern Data Engineering for AI Systems" (5-day capstone)
**Status:** work in progress, built incrementally

---

## 1. Goal

Build and demonstrate, with real infrastructure and executed evidence, an
end-to-end data platform that:

1. Ingests e-commerce transactions through a real Kafka broker, validating
   every record against a data contract at the ingestion boundary.
2. Lands validated data in a Bronze → Silver → Gold Delta Lakehouse, with a
   genuine `MERGE` on a business key and enforced schema.
3. Serves a Retrieval-Augmented Generation (RAG) assistant over the Gold
   layer, using hybrid search (dense + BM25) fused with Reciprocal Rank
   Fusion (RRF) and re-ranked with a cross-encoder, returning answers
   grounded with citations.
4. Orchestrates all of the above as an Airflow DAG (DAG = Directed Acyclic
   Graph) where a failed quality gate halts every downstream task.
5. Gates data quality with Great Expectations (GE) and emits OpenLineage
   (OL) START/COMPLETE/FAIL events per stage for lineage tracking.

The platform must prove each capability with **executed output**, not
description — per the grading rule that simulated components score zero.

## 2. Scope

- Real Kafka (KRaft mode, no ZooKeeper), real Delta Lake tables via PySpark,
  real Great Expectations 1.x fluent checkpoints, real OpenLineage client
  events, real Airflow DAG execution, real ChromaDB + BM25 + cross-encoder
  retrieval.
- One dataset: UCI Online Retail (real transactions, real data-quality
  defects — no synthetic data invented to make the demo easier).
- Two demo runs (clean, and a deliberate quality-gate failure) plus a reset
  path, all triggered through the same DAG.
- Evidence captured under `evidence/` for every graded deliverable.

## 3. Non-goals

- No production security hardening (`admin`/`admin` Airflow credentials are
  explicitly dev-only, not something to harden for this capstone).
- No horizontal scaling, multi-broker Kafka cluster, or cloud deployment —
  everything runs in a single `docker compose` stack on one host.
- No generative LLM call in the answer path — the RAG answer is
  **extractive** (retrieved chunk text stitched with citations), not
  LLM-authored. This keeps the graded RAG surface (retrieval quality,
  fusion, reranking, grounding) verifiable without an external API
  dependency.
- No customer-facing UI — the deliverables are demonstrated via CLI runs,
  the Airflow UI, and captured evidence artifacts.
- No handling of currencies/locales beyond the source dataset (GBP, UK
  retailer data).

---

## 4. Graded Deliverables

### 4.1 Ingestion — Kafka + contract validation (20 pts)

| Requirement | Acceptance Criteria | Evidence Artifact |
|---|---|---|
| Real Kafka producer/consumer | `producer.py` publishes UCI retail rows as JSON events to topic `retail_transactions` on a real KRaft broker (`apache/kafka:latest`); `consumer.py` consumes with `group_id="capstone-ingest"` and terminates after 3 idle polls | `evidence/ingestion/01_producer_batchA.log`, `evidence/ingestion/02_consumer_batchA.log` |
| Schema validation at the boundary | Every consumed record is validated against the Pydantic `RetailTransaction` contract (`src/contracts.py`) before it can reach landing/Bronze | `evidence/ingestion/02_consumer_batchA.log` (accepted/rejected counts) |
| Malformed → quarantine with reason | Records failing validation are never dropped silently: they are published to dead-letter topic `retail_transactions_dlq` with a `rejection_reason` string and the original `source_offset`/`source_partition` | `evidence/ingestion/dlq_contents.jsonl`, `evidence/ingestion/03_dlq_dump.log` |
| Demonstrated failure path | Running the producer with `--inject-malformed 3` (invalid invoice format, unparseable date, non-numeric quantity) produces exactly 3 dead-lettered messages, each with a distinct reason | `evidence/ingestion/dlq_contents.jsonl` (3 entries, one per injected defect) |

### 4.2 Delta Lakehouse — Bronze/Silver/Gold (25 pts)

| Requirement | Acceptance Criteria | Evidence Artifact |
|---|---|---|
| Bronze layer (raw + audit) | `bronze.py` appends validated landing JSONL as-is (no casts, no dedup) plus an `ingest_ts` column; duplicates from re-delivered Kafka messages are legitimately preserved | Bronze Delta table under `data/lake/bronze` (row counts printed in task logs) |
| Silver layer with real `MERGE` | `silver.py` deduplicates, parses dates, aggregates to grain `(InvoiceNo, StockCode)`, then executes a real Delta `MERGE` (`whenMatchedUpdateAll().whenNotMatchedInsertAll()`) against the existing Silver table on the business key | `evidence/delta/merge_metrics_<ts>.json` (numTargetRowsUpdated / numTargetRowsInserted / numSourceRows) |
| Schema enforcement demonstrated | `schema_demo.py` attempts to append a row with an undeclared column (`discount`) to Silver; Delta refuses the write and the captured error is saved | `evidence/delta/schema_rejection_<ts>.txt` |
| Gold is a genuine aggregate | `gold.py` computes `GROUP BY StockCode` aggregates (total revenue, total quantity, invoice count, top-3 countries by revenue, a dense `revenue_rank` window function) — not a copy of Silver | Gold Delta table under `data/lake/gold`; top-5-by-revenue table printed in `bronze_to_silver`/`silver_to_gold` task logs |

### 4.3 RAG Pipeline (25 pts)

| Requirement | Acceptance Criteria | Evidence Artifact |
|---|---|---|
| Chunking | `build_index.py` splits each multi-sentence Gold-derived document into sentence-level chunks (`chunk_size=2`, 1-sentence overlap) — verified by "N chunks" log per document | `evidence/rag/build_index.log` (task stdout) |
| Embeddings + vector store | Chunks are embedded with `all-MiniLM-L6-v2` (sentence-transformers) and persisted in a real ChromaDB `PersistentClient` collection (`rag_capstone_chunks`) | Chroma collection under `data/chroma`; `evidence/rag/build_index.log` |
| Hybrid search fused with RRF | `answer.py` runs dense vector search (top 6) and BM25 keyword search (top 6) independently, then fuses them with Reciprocal Rank Fusion, `k=60`, to a shortlist of 6 | `evidence/rag/answer_<query>.log` with `--compare` output showing distinct BM25/Vector/RRF top-3 ids |
| Cross-encoder reranking | The RRF shortlist is re-scored by `cross-encoder/ms-marco-MiniLM-L-6-v2` and cut to the top 3 | Same `--compare` log — "Rerank" column differs from "RRF" column, proving reranking changes order |
| Grounded answers with citations | `compose_answer()` stitches the top-3 reranked chunk texts with `[Source N]` markers and lists each source's chunk id, doc id, and full chunk text — extractive, no hallucinated content | `evidence/rag/answer_<query>.log` (full answer + source list) |

### 4.4 Orchestration — Airflow DAG (15 pts)

| Requirement | Acceptance Criteria | Evidence Artifact |
|---|---|---|
| Single DAG wiring all stages | `capstone_pipeline` DAG chains `produce → consume → bronze → gate_bronze → silver → maybe_inject_corruption → gate_silver → gold → rag_docs → rag_index` with default `trigger_rule="all_success"` | `dags/capstone_pipeline.py`; Airflow UI graph view screenshot |
| Correct dependencies | Each task only starts after its upstream completes; parameterized via `batch`, `inject_malformed`, `inject_corruption` at trigger time | `evidence/airflow_logs/dag_id=capstone_pipeline/run_id=.../` (per-task logs, one folder per run) |
| Failed gate halts downstream | When `ge_gate.py` exits 1 (via `sys.exit(1)` on a failed GE checkpoint), the task is marked `failed` and every downstream task (`silver_to_gold`, `build_rag_documents`, `build_rag_index`) is marked `upstream_failed`, never runs | `evidence/airflow_logs/dag_id=capstone_pipeline/run_id=<corruption-run>/` showing `ge_gate_silver` failed and later tasks absent/upstream_failed |
| Maintenance DAG | Separate `reset_demo_state` DAG deletes `_injected=true` rows from Silver so the pipeline can be re-demonstrated cleanly | `evidence/airflow_logs/dag_id=reset_demo_state/...` |

### 4.5 Quality + Lineage — Great Expectations + OpenLineage (15 pts)

| Requirement | Acceptance Criteria | Evidence Artifact |
|---|---|---|
| GE checks that actually gate | `ge_gate.py` runs a real GX 1.x fluent checkpoint (`gx.Checkpoint`, not a hand-rolled check) per layer; the checkpoint's `result.success` controls the process exit code | `evidence/ge_lineage/checkpoint_bronze_pass_<ts>.json`, `checkpoint_silver_pass_<ts>.json` |
| Silver uniqueness gate fires on corruption | `ExpectCompoundColumnsToBeUnique(InvoiceNo, StockCode)` on Silver fails after `inject_corruption.py` appends duplicate keys | `evidence/ge_lineage/checkpoint_silver_fail_<ts>.json` |
| OpenLineage START/COMPLETE/FAIL per stage | Every stage script wraps its work in `lineage.stage(job_name)`, emitting a real `openlineage-python` `RunEvent` (`event_v2` API) at START, then COMPLETE or FAIL, all sharing one `runId` per pipeline run | `evidence/ge_lineage/openlineage_events.jsonl` |
| Unified run correlation | `OL_RUN_ID` is set once per Airflow DAG run (`{{ run_id }}` template) and read by every stage via `lineage.get_run_id()`, so all events for one run share a `runId` | Filter `openlineage_events.jsonl` by `run.runId` — all stages of one DAG run appear together, including the `FAIL` event for `ge_gate_silver` |

---

## 5. Data Decisions

**Dataset:** [UCI Online Retail](https://archive.ics.uci.edu/dataset/352/online+retail)
— 541,909 real transactions from a UK online retailer (2010–2011). Chosen
because it has *genuine* quality problems (missing CustomerID in ~25% of
rows, negative quantities, duplicate rows) rather than needing synthetic
defects invented for the demo. Citation (CC BY 4.0): Chen, D. (2015).
*Online Retail* [Dataset]. UCI Machine Learning Repository.
https://doi.org/10.24432/C5BW33

**Contract scope — structural errors only.** The Pydantic contract
(`src/contracts.py`) rejects a record only when it is structurally unusable:

- `InvoiceNo` not matching `^[A-Za-z]?\d{5,6}$` (invalid invoice format)
- `InvoiceDate` not parseable as `M/D/YYYY H:MM`
- `Quantity` not an integer
- `StockCode` / `Country` blank or missing

**Negative quantities are NOT rejected at the contract.** In this dataset
they represent genuine cancellations (a business-valid record type, invoice
numbers prefixed `C`), not malformed data. Rejecting them at the boundary
would discard legitimate business events. Instead:

- They pass the contract and reach Bronze/Silver unchanged.
- Silver flags them via a derived `is_cancellation` boolean
  (`Quantity < 0 OR InvoiceNo starts with "C"`).
- Gold excludes cancellations (and demo-injected rows) from all revenue and
  quantity aggregates, so reported totals reflect genuine sales only.

**Silver grain: one row per `(InvoiceNo, StockCode)`.** The raw feed can
contain the same product listed twice within one invoice (duplicate line
items), and Delta's `MERGE` throws `multiple source rows matched` if the
source isn't already unique on the merge key. Silver therefore pre-aggregates
to that grain before every `MERGE`, with these deterministic rules
(documented in `src/silver.py` for the evaluator):

| Column | Rule |
|---|---|
| `Quantity` | `SUM` |
| `line_revenue` | `SUM(Quantity * UnitPrice)` |
| `UnitPrice` | derived: `line_revenue / Quantity` (falls back to first observed price if quantity nets to 0) |
| `invoice_ts` | `MIN(parsed InvoiceDate)` |
| `Description` / `CustomerID` / `Country` | `FIRST(..., ignorenulls=True)` |
| `is_cancellation` | `MAX(...)` (any negative-qty or `C`-invoice line marks the whole grain row) |

**Non-product StockCodes** (`POST`, `D`, `BANK CHARGES`, etc.) are real
charges but not products. They fail the product-code regex
(`^\d{5}[A-Za-z]?$`) and are routed to a side table
(`data/lake/silver_non_product`) instead of the main Silver table, keeping
product analytics and the RAG corpus free of non-product noise.

---

## 6. Demo Plan

**Run 1 — clean pipeline (all green).**

1. Trigger `capstone_pipeline` with default params (`batch="A"`,
   `inject_malformed=3`, `inject_corruption=False`).
2. Every task succeeds: 3 deliberately malformed messages are dead-lettered
   with reasons (proving the contract works even on a clean run); both GE
   gates (`ge_gate_bronze`, `ge_gate_silver`) pass; Gold and the RAG index
   are rebuilt.
3. Query the assistant, e.g. `python src/rag/answer.py "which product has
   the highest revenue?" --compare`, and confirm the answer cites specific
   `[Source N]` chunks traceable to Gold rows.

**Run 2 — quality-gate failure (deliberate).**

1. Trigger `capstone_pipeline` with `inject_corruption=True`.
2. `maybe_inject_corruption` runs `inject_corruption.py`, appending 5
   duplicate `(InvoiceNo, StockCode)` rows into Silver.
3. `ge_gate_silver`'s `ExpectCompoundColumnsToBeUnique` expectation fails →
   checkpoint `success=False` → `sys.exit(1)` → task `ge_gate_silver` fails.
4. Downstream tasks (`silver_to_gold`, `build_rag_documents`,
   `build_rag_index`) are marked `upstream_failed` and never execute —
   captured in the Airflow UI and task logs as the failure-path evidence.

**Reset.**

1. Trigger the `reset_demo_state` DAG, which runs `reset_demo.py` to delete
   all `_injected=true` rows from Silver.
2. The next `capstone_pipeline` run passes `ge_gate_silver` again, restoring
   the demo to a repeatable clean state.
