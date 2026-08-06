# Roadmap — completing the platform (branch: `feature/complete-platform`)

> `main` is frozen: it is the evaluated, passing submission. Everything here
> happens on this branch, for learning and for v4 of the association assistant.
>
> **Ordering rule**: nothing that touches the shared Docker environment or the
> Delta tables runs before the capstone presentation is over.

## Tier 1 — completes the pipeline itself (highest value)

| # | Gap today | What to build | Why it matters to you |
|---|---|---|---|
| 1 | No **generation** — answers are extractive | Wire `build_rag_prompt` (Day-3 lab) to a real LLM API; keep citations, add a "grounded-only" system prompt | Completes the G in RAG. v4 needs it; v3 already does this, so it is familiar ground |
| 2 | Lineage events go to a **file**, no graph | Add a Marquez container to compose, switch OpenLineage transport from `FileTransport` to HTTP, and **populate `inputs`/`outputs`** per stage (bronze/silver/gold paths, Kafka topics) | Turns "من أين جاءت هذه الإجابة؟" into a clickable graph — the exact audit need of the association's content |
| 3 | No **answer-quality** metrics | RAGAS: context precision/recall, answer faithfulness/relevance over a small golden set | Measures hallucination — impossible to measure while answers were extractive (that is why it comes after #1) |
| 4 | No **latency** measurement | Time each retrieval stage, log p50/p95/p99 to a Delta table, chart it | v3 has zero latency measurement today — documented pain point |

## Tier 2 — Airflow patterns we deliberately skipped (pure learning)

| # | Pattern | Concretely here |
|---|---|---|
| 5 | `retries` + `retry_delay` + exponential backoff | Retry the Kafka stages (transient broker errors) but **never** the quality gates — a gate must fail loudly |
| 6 | `XCom` | Pass row counts between tasks (e.g. bronze → gate) instead of re-reading the table; keep payloads tiny (ids/counts, never DataFrames) |
| 7 | `BranchPythonOperator` | Replace the Jinja `{% if %}` trick for corruption injection with a real branch |
| 8 | Sensors | A sensor that waits for the landing file / for Kafka lag to hit zero, instead of the idle-poll counter |
| 9 | `TaskGroup` | Group the DAG into ingest / lakehouse / rag clusters — a much cleaner graph view |
| 10 | `on_failure_callback` + SLA | Emit an alert (or a log line) when a gate trips or a stage runs long |

## Tier 3 — deeper data engineering

| # | Gap | What to build |
|---|---|---|
| 11 | Consumer is **batch**, not streaming | Spark Structured Streaming: `readStream` from Kafka → `writeStream` to bronze with checkpointing, watermarks, and exactly-once semantics |
| 12 | Source is a replayed CSV, not **CDC** | Postgres + Debezium → Kafka, then MERGE the change events into silver (insert/update/delete) |
| 13 | Delta maintenance untouched | `OPTIMIZE` + `Z-ORDER BY` and **measure the query-time difference**; `VACUUM` with retention; a real time-travel restore after a simulated bad load |
| 14 | Schema evolution only refused, never accepted | Add a column with `mergeSchema=true` and show old rows reading back as NULL — the safe migration path |

## Tier 4 — advanced RAG

| # | Gap | What to build |
|---|---|---|
| 15 | No metadata filtering | Chroma `where={"country": ...}` filters, and compare recall with/without |
| 16 | Flat indexing only | Parent-child (retrieve small, return large) and summary indexing from Day 3 |
| 17 | Evaluation is ad-hoc | A golden set of ~20 question/answer pairs with Hit@k and MRR — same discipline as v3's `tests/eval_retrieval.py` |

## What each tier teaches, honestly

- **Tier 1** turns the project into something you would actually run for the
  association: it answers, it proves it did not hallucinate, and its lineage is
  visible.
- **Tier 2** is the Airflow depth the rubric never asked for — cheap to build,
  and it is what separates "I wrote a DAG" from "I operate pipelines".
- **Tier 3** is the real engineering: streaming semantics and CDC are the two
  hardest ideas in the whole course, and both are still theoretical for us.
- **Tier 4** is where v4's retrieval quality actually improves.

## Suggested order

1 → 2 → 5,6,7,9 (a single Airflow refactor pass) → 3 → 11 → 13 → 4 → the rest.

Reason: #1 and #2 make the system *feel* complete and are the shortest paths;
the Airflow patterns are a single afternoon; streaming (#11) is the biggest
conceptual jump and deserves a clear head.
