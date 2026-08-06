# CLAUDE.md — SDAIA Capstone: Real-Time E-Commerce Data Platform with RAG

Graded capstone (rubric: 100 pts, pass ≥60) for SDAIA "Modern Data Engineering
for AI Systems". Owner: عبدالعزيز (student, communicates in Arabic — pair every
Arabic technical term with its English original). Planning docs live in the
course folder: `C:\Users\abdul\OneDrive\Islamic Content Services Associations
In Languages\SDAIA Modern Data Engineering\` (see `خطة المشروع الختامي.md` and
`الدفاع والأسئلة المتوقعة.md` there, plus the course CLAUDE.md).

## Non-negotiable workflow rules

1. **Commit + push IMMEDIATELY after every completed piece** — the rubric
   grades "incremental commit history (not a single bulk upload)". One topic
   per commit. Never rewrite history (`--amend`/rebase of pushed commits).
2. **Evidence discipline**: every claim needs an executed-run artifact under
   `evidence/{ingestion,delta,rag,airflow,ge_lineage}/`, captured the moment
   it happens. Evidence is deliberately git-tracked (`!evidence/**` in
   .gitignore) except Airflow scheduler/parser chatter.
3. **Real libraries only** — a simulation earns zero. kafka-python, pyspark +
   delta-spark, chromadb, apache-airflow, great_expectations==1.19.1,
   openlineage-python.
4. **Failure paths are graded**: keep both the all-green run AND the
   gate-halt run (inject_corruption=true → ge_gate_silver fails → downstream
   upstream_failed) demonstrable. `reset_demo_state` DAG cleans injected rows.

## Architecture in one line

producer → Kafka (`retail_transactions`) → consumer + Pydantic contract
(malformed → `retail_transactions_dlq` with reason) → landing JSONL → bronze
(Delta, untransformed audit) → GE gate → silver (dedupe → date parse → grain
aggregation → MERGE on InvoiceNo+StockCode) → GE gate (uniqueness) → gold
(overwrite, ranked aggregate) → RAG docs → chunks (contextual headers) →
ChromaDB + BM25 → RRF(k=60) → cross-encoder → cited extractive answers.
Every executing stage emits OpenLineage START/COMPLETE/FAIL via
`src/lineage.py` under one uuid5-derived runId per DAG run.

## Environment & commands

- Everything runs in Docker: `docker compose up -d` (kafka + airflow).
  Airflow UI: localhost:8080, admin/admin. **Spark runs ONLY inside the
  container** — never write the same Delta table from Windows and the
  container (corruption).
- Heavy deps live in `/opt/venvs/compute` inside the airflow container
  (isolated from Airflow's pinned env). DAG tasks call
  `/opt/venvs/compute/bin/python /opt/capstone/src/<script>.py`.
- Run a stage manually:
  `docker exec capstone-airflow /opt/venvs/compute/bin/python /opt/capstone/src/rag/answer.py "question" --compare`
- Tests (host-only, tests/ not mounted): `%USERPROFILE%\sdaia-venv\Scripts\python.exe -m pytest tests -q` (23 tests).
- Trigger runs: defaults = clean batch A; `{"batch":"B"}` = MERGE
  update+insert proof; `{"inject_corruption":true}` = failure demo.

## Hard-won gotchas (each cost us a failed run tonight)

- **PowerShell + UTF-8**: never `Get-Content | -replace | Set-Content` on
  files with Arabic/emoji — it mojibakes them (happened to README, commit
  "Fix README encoding"). Use the Write/Edit tools or python.
- **PowerShell truncation**: `cmd | Tee-Object file | Select-Object -First N`
  kills the upstream pipe — the file gets truncated (happened to RAG
  evidence). Write the file fully first, display separately.
- **OpenLineage runIds must be UUIDs**: Airflow's `manual__...` run id is
  mapped via `uuid.uuid5(NAMESPACE_URL, run_id)` in lineage.py.
- **ChromaDB caps one .add() at ~5461 items** — batch in 5000s.
- **Delta MERGE explodes on duplicate source keys** — silver pre-aggregates
  to (InvoiceNo, StockCode) grain BEFORE merging.
- **GE gates must be able to fail**: bronze gate has contract-level checks
  only (bronze legitimately holds duplicates); uniqueness lives on the
  silver gate. Gates halt Airflow via `sys.exit(1)` + default trigger rules.
- **mp4 for GitHub**: re-encode with `-movflags +faststart` or the embedded
  player stalls.
- **PPTX Arabic**: mixed Arabic/English runs need RLM (U+200F) at boundary
  transitions; editing mixed text directly in PowerPoint can re-glue words.

## Data decisions (defend these, don't "fix" them)

- Contract rejects STRUCTURAL errors only (bad InvoiceNo format, unparseable
  date, non-numeric quantity, blank required fields). Negative quantities are
  genuine cancellations: they pass, get flagged `is_cancellation` in silver,
  and are excluded from gold revenue.
- Bronze is contract-typed but otherwise untransformed (audit layer keeps
  duplicates + raw date strings as proof).
- Non-product StockCodes (POST, D, BANK CHARGES…) → `silver_non_product`.
- Silver aggregation is order-independent (MIN/SUM only — no F.first()).
- Top-rank gold docs spell out their status in words ("the number one
  best-selling…") because numeric ranks barely move embeddings; answer.py
  additionally expands superlative queries before retrieval.
