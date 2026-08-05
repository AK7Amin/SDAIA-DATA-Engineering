# Architecture

**Project:** Real-Time E-Commerce Data Platform with RAG
**Program:** SDAIA Academy — "Modern Data Engineering for AI Systems" capstone

---

## 1. Component Diagram

```
 UCI Online Retail CSV
         │
         │  producer.py  (KafkaProducer, JSON value_serializer)
         ▼
 ┌───────────────────────────────────────────────────────────────────┐
 │  Kafka (KRaft, single broker, apache/kafka:latest)                │
 │  topic: retail_transactions        topic: retail_transactions_dlq │
 │  listeners: kafka:9092 (internal) / localhost:29092 (host)        │
 └───────────────────────────────────────────────────────────────────┘
         │  consumer.py  (group_id=capstone-ingest, idle-poll exit)
         ▼
 ┌─────────────────────────────┐        invalid record
 │ Pydantic contract           │───────────────────────────┐
 │ RetailTransaction            │                            ▼
 │ (src/contracts.py)          │                 DLQ producer.send(
 └─────────────────────────────┘                   rejected_record,
         │ valid                                    rejection_reason,
         ▼                                          source_offset/partition)
 data/landing/*.jsonl  (validated_<ts>.jsonl)
         │
         │  bronze.py  (Spark read.json → append, + ingest_ts)
         ▼
 ┌───────────────────────────┐
 │ Bronze (Delta)            │  raw + audit — no casts, no dedup,
 │ data/lake/bronze          │  duplicates from re-delivery kept as proof
 └───────────────────────────┘
         │
         │  ge_gate.py --layer bronze  (GX checkpoint: not-null, invoice regex)
         ▼  [gate must pass — sys.exit(1) on failure]
         │
         │  silver.py:
         │    dedup(BUSINESS_COLS) → route non-product StockCodes aside →
         │    parse invoice_ts → flag is_cancellation → aggregate to grain
         │    (InvoiceNo, StockCode) → Delta MERGE (update/insert)
         ▼
 ┌───────────────────────────┐      ┌─────────────────────────────┐
 │ Silver (Delta)            │      │ silver_non_product (Delta)  │
 │ data/lake/silver          │      │ POST/D/BANK CHARGES rows    │
 └───────────────────────────┘      └─────────────────────────────┘
         │
         │  maybe_inject_corruption  (demo only: inject_corruption.py
         │  appends duplicate (InvoiceNo, StockCode) rows, _injected=true)
         ▼
         │  ge_gate.py --layer silver
         │    (GX checkpoint: ExpectCompoundColumnsToBeUnique,
         │     invoice_ts not null)
         ▼  [gate must pass — trips on injected duplicates]
         │
         │  gold.py:
         │    filter out is_cancellation / _injected →
         │    GROUP BY StockCode (revenue, quantity, invoices,
         │    top-3 countries by revenue) → revenue_rank window
         ▼
 ┌───────────────────────────┐
 │ Gold (Delta)              │  genuine aggregate, ranked
 │ data/lake/gold            │
 └───────────────────────────┘
         │
         │  rag/docs_from_gold.py  (1 doc per product, 4-6 sentences,
         │                          top 1000 by revenue)
         ▼
 data/rag_docs.jsonl
         │
         │  rag/build_index.py:
         │    chunk_documents (sentence-level, size=2, 1-sentence overlap)
         ▼
 ┌────────────────────────┐   ┌──────────────────────────┐
 │ ChromaDB               │   │ BM25Okapi index          │
 │ (all-MiniLM-L6-v2      │   │ (data/bm25.pkl, pickled  │
 │  embeddings, persisted │   │  with chunk list)        │
 │  at data/chroma)       │   │                          │
 └────────────────────────┘   └──────────────────────────┘
         │                              │
         └────────────┬─────────────────┘
                       │  rag/answer.py:
                       │    vector_search(top 15) + bm25_search(top 15)
                       │    → reciprocal_rank_fusion(k=60, top 12)
                       │    → cross-encoder rerank (ms-marco-MiniLM-L-6-v2, top 3)
                       ▼
              compose_answer(): extractive answer,
              [Source N] citations + full source list

 Cross-cutting: every stage above runs inside `with lineage.stage(job_name):`
 → emits OpenLineage START, then COMPLETE or FAIL, all sharing one
 OL_RUN_ID per pipeline run (evidence/ge_lineage/openlineage_events.jsonl).
```

### Airflow DAG shape (`dags/capstone_pipeline.py`)

```
produce_to_kafka
  → consume_validate_to_landing
    → bronze_ingest
      → ge_gate_bronze
        → bronze_to_silver
          → maybe_inject_corruption   (no-op unless params.inject_corruption)
            → ge_gate_silver
              → silver_to_gold
                → build_rag_documents
                  → build_rag_index
```

All tasks keep the Airflow default `trigger_rule="all_success"`. A gate
script's `sys.exit(1)` fails its task, which marks every task after it
`upstream_failed` — the DAG never "skips past" a failed gate. A separate
`reset_demo_state` DAG (single task, `reset_demo.py`) deletes
`_injected=true` rows from Silver to restore a clean state between demo
runs.

---

## 2. Runtime Layout

`docker-compose.yml` defines two services:

| Service | Image / Build | Purpose |
|---|---|---|
| `kafka` | `apache/kafka:latest` | KRaft-mode broker (no ZooKeeper). Dual listeners: `PLAINTEXT://:9092` for other containers, `PLAINTEXT_HOST://:29092` for the host. Healthcheck polls `kafka-broker-api-versions.sh`; `airflow` waits on `service_healthy` before starting. |
| `airflow` | `./docker` (custom Dockerfile) | Airflow 2.10.5 standalone (webserver + scheduler + triggerer in one process), runs the DAG above via `BashOperator` tasks. |

**Why a separate compute venv.** The Airflow image is built `FROM
apache/airflow:2.10.5-python3.12` with tightly pinned dependencies that
Airflow itself needs. Rather than risk breaking that environment with heavy,
fast-moving packages (PySpark, Delta, torch, ChromaDB, sentence-transformers,
Great Expectations), the Dockerfile creates an isolated venv at
`/opt/venvs/compute` and installs all pipeline dependencies there. Every DAG
task is a `BashOperator` that shells out to
`/opt/venvs/compute/bin/python <script>` — Airflow's own environment never
imports any of these libraries directly, and rebuilding the Airflow image
after a pipeline dependency change doesn't require touching Airflow's core
install.

**Volume mounts** (host path → container path):

| Host | Container | Purpose |
|---|---|---|
| `./dags` | `/opt/airflow/dags` | DAG definitions, hot-reloaded by the scheduler |
| `./src` | `/opt/capstone/src` | All pipeline stage scripts, importable by the compute venv |
| `./data` | `/opt/capstone/data` | Raw CSV, landing JSONL, Delta tables (bronze/silver/gold), Chroma store, BM25 pickle |
| `./evidence` | `/opt/capstone/evidence` | Captured proof of executed runs (ingestion logs, DLQ dump, MERGE metrics, GE checkpoint JSON, OpenLineage events) |
| `./evidence/airflow_logs` | `/opt/airflow/logs` | Airflow's own task/scheduler logs, redirected into `evidence/` so DAG run history (including `upstream_failed` states) is captured as graded evidence rather than living only inside the ephemeral container |

`evidence/` is the one generated-artifact directory deliberately *not*
gitignored (`.gitignore` excludes `data/`, `logs/`, `*.log`, `chroma/`,
`spark-warehouse/` but has `!evidence/**` to force-track it) — it is the
rubric's proof that everything above actually ran.

---

## 3. Environment Variables

| Variable | Value | Set by | Purpose |
|---|---|---|---|
| `KAFKA_BOOTSTRAP` | `kafka:9092` in-container / `localhost:29092` on host (default in `src/config.py`) | `docker-compose.yml` (airflow service) / defaulted in `config.py` for host runs | Broker address for producer/consumer; dual value avoids editing code between container and host execution |
| `CAPSTONE_HOME` | `/opt/capstone` in-container / repo root on host (defaulted in `config.py`) | `docker-compose.yml` | Base path all other `DATA_DIR`/`EVIDENCE_DIR` paths are derived from |
| `OL_RUN_ID` | `{{ run_id }}` (Airflow's own run id, templated per DAG run) | `dags/capstone_pipeline.py` (`COMMON["env"]`) | Shared OpenLineage `runId` across every stage of one pipeline run, so lineage events for bronze/silver/gates/RAG correlate into one run graph. Outside Airflow, `lineage.get_run_id()` falls back to a UUID persisted at `data/current_run_id.txt` for the duration of one manual demo session. |
| `PYSPARK_PYTHON` / `PYSPARK_DRIVER_PYTHON` | `/opt/venvs/compute/bin/python` | `docker/Dockerfile` (`ENV`) | Forces Spark workers and driver to use the compute venv's Python (with pyspark/delta-spark installed), not Airflow's own interpreter |
| `_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD` | `admin` / `admin` | `docker-compose.yml` | Airflow UI login for the `standalone` command — **dev-only**, not hardened, appropriate only for a local capstone demo |
| `AIRFLOW__CORE__LOAD_EXAMPLES` | `false` | `docker-compose.yml` | Keeps the DAG list limited to the capstone's own two DAGs |
| `PYTHONIOENCODING` | `utf-8` | `dags/capstone_pipeline.py` (`COMMON["env"]`) | Avoids Windows-console-style encoding errors for any non-ASCII output when tasks run |

---

## 4. Version Pins and Why

| Package | Version | Reason |
|---|---|---|
| `pyspark` | `3.5.9` | Paired specifically with `delta-spark==3.2.0` — the Delta Lake compatibility matrix ties Delta 3.2.x to Spark 3.5.x; mismatched pairs fail at `configure_spark_with_delta_pip()` or silently produce non-Delta writes. `3.5.9` (not the `3.5.0` used in earlier course labs) fixes a Python 3.12 worker crash present in `3.5.0` (workers die silently under CPython 3.12). |
| `delta-spark` | `3.2.0` | Matches `pyspark==3.5.9` per Delta's published compatibility table; provides `DeltaTable.merge()`, schema enforcement, and time-travel history (`target.history(1)`) used by `silver.py`'s MERGE-metrics evidence. |
| `great_expectations` | `1.19.1` | Pinned to the GX 1.x "fluent" API (`gx.ExpectationSuite`, `gx.Checkpoint`, `context.data_sources.add_pandas(...)`) that `src/ge_gate.py` is written against; the 0.x API has an incompatible object model (`context.add_expectation_suite`, class-based `BatchRequest`) and would require a rewrite. |
| `torch` | CPU build, installed from `--index-url https://download.pytorch.org/whl/cpu` | The default PyPI index resolves `torch` to a multi-gigabyte CUDA-enabled wheel even on a CPU-only container; installing from the CPU-only index first (before `sentence-transformers`/`chromadb`, which would otherwise pull the CUDA wheel as a transitive dependency) keeps the image build fast and avoids downloading unusable GPU binaries. |

---

## 5. Key Design Defenses

**Why Bronze stays raw while the contract rejects at the boundary.**
The rubric's contract requirement is a *syntactic* gate at ingestion —
records must be well-typed and well-formed before they are allowed past the
consumer at all. That gate lives in `src/contracts.py`
(`RetailTransaction`), enforced by `consumer.py` before anything is written
to `data/landing/`. Bronze, by contrast, is the audit layer: once a record
has passed the contract, Bronze preserves it exactly as validated —
contract-typed by `model_dump()`, plus Kafka offset/partition and an
`ingest_ts`, but with no dedup, no date parsing, and no aggregation —
including duplicates from Kafka re-delivery or overlapping batches. If Bronze also deduplicated or cast types, there would be no
raw-layer evidence left to show the evaluator that ingestion actually saw
what came off the wire — the medallion architecture's audit guarantee would
collapse into "Bronze is just a slightly-earlier Silver." Keeping the two
concerns (contract = syntactic validity; Bronze = unmodified audit copy)
separate is what makes both defensible independently.

**Why pre-aggregation happens before `MERGE`.**
The raw feed can contain the same `(InvoiceNo, StockCode)` combination more
than once in a single landing increment (e.g. the same product listed twice
on one invoice, or overlapping batches resent for correction). Delta's
`MERGE` requires the *source* side to already be unique on the join key —
if it isn't, Delta raises `UnsupportedOperationException: … multiple source
rows matched …` and aborts the whole operation. `silver.py` therefore always
performs a deterministic `groupBy("InvoiceNo", "StockCode").agg(...)` step
(documented rules in `src/silver.py` and mirrored in `docs/PRD.md` §5)
*before* constructing the `MERGE`, guaranteeing the source is grain-unique
every time regardless of how the landing increment was produced upstream.

**Why the quality gates call `sys.exit(1)` instead of just logging a
warning.** Airflow's `BashOperator` tasks report failure purely through the
wrapped process's exit code — there is no other signal an operator inspects
by default. `ge_gate.py` runs a real GX checkpoint and inspects
`result.success`; if it is `False`, the script calls `sys.exit(1)` from
*inside* the `lineage.stage(...)` context manager, so the `SystemExit`
(a `BaseException`, not just an `Exception` — deliberately caught by
`stage()`'s `except BaseException` clause) still triggers an OpenLineage
`FAIL` event before propagating out of the process. Airflow sees a non-zero
exit code, marks the task `failed`, and — because every task in the DAG
keeps the default `trigger_rule="all_success"` — every task downstream of
the failed gate is marked `upstream_failed` and never runs. This is the
entire mechanism behind the "failed gate halts downstream" deliverable: no
custom Airflow sensor or branching operator is needed, only a script that
exits honestly.
