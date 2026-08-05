"""Capstone DAG — wires every stage; a failed quality gate halts downstream.

Halting relies on two defaults we deliberately do NOT touch:
  - every task keeps trigger_rule='all_success'
  - gate scripts sys.exit(1) on failure -> task fails -> downstream upstream_failed

Params (set at trigger time in the UI):
  batch:             "A" (initial load) or "B" (overlapping corrections -> MERGE updates)
  inject_malformed:  how many broken messages the producer adds (DLQ demo)
  inject_corruption: True only for the failure-demo run — duplicates appended
                     straight into Silver AFTER dedup, so the silver gate trips.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

PY = "/opt/venvs/compute/bin/python"
SRC = "/opt/capstone/src"

COMMON = {
    "append_env": True,
    "env": {
        "OL_RUN_ID": "{{ run_id }}",
        "PYTHONIOENCODING": "utf-8",
    },
}

with DAG(
    dag_id="capstone_pipeline",
    start_date=datetime(2026, 8, 1),
    schedule=None,
    catchup=False,
    # two concurrent runs would share the Kafka consumer group and the
    # landing directory and cross-contaminate each other's increments
    max_active_runs=1,
    params={"batch": "A", "inject_malformed": 3, "inject_corruption": False},
    tags=["capstone"],
) as dag:

    produce = BashOperator(
        task_id="produce_to_kafka",
        bash_command=(
            f"{PY} {SRC}/producer.py --batch {{{{ params.batch }}}} "
            f"--inject-malformed {{{{ params.inject_malformed }}}}"
        ),
        **COMMON,
    )

    consume = BashOperator(
        task_id="consume_validate_to_landing",
        bash_command=f"{PY} {SRC}/consumer.py",
        **COMMON,
    )

    bronze = BashOperator(
        task_id="bronze_ingest",
        bash_command=f"{PY} {SRC}/bronze.py",
        **COMMON,
    )

    gate_bronze = BashOperator(
        task_id="ge_gate_bronze",
        bash_command=f"{PY} {SRC}/ge_gate.py --layer bronze",
        **COMMON,
    )

    silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=f"{PY} {SRC}/silver.py",
        **COMMON,
    )

    inject = BashOperator(
        task_id="maybe_inject_corruption",
        bash_command=(
            "{% if params.inject_corruption %}"
            f"{PY} {SRC}/inject_corruption.py"
            "{% else %}echo 'corruption injection skipped (clean run)'{% endif %}"
        ),
        **COMMON,
    )

    gate_silver = BashOperator(
        task_id="ge_gate_silver",
        bash_command=f"{PY} {SRC}/ge_gate.py --layer silver",
        **COMMON,
    )

    gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=f"{PY} {SRC}/gold.py",
        **COMMON,
    )

    rag_docs = BashOperator(
        task_id="build_rag_documents",
        bash_command=f"{PY} {SRC}/rag/docs_from_gold.py",
        **COMMON,
    )

    rag_index = BashOperator(
        task_id="build_rag_index",
        bash_command=f"{PY} {SRC}/rag/build_index.py",
        **COMMON,
    )

    (produce >> consume >> bronze >> gate_bronze >> silver
     >> inject >> gate_silver >> gold >> rag_docs >> rag_index)


with DAG(
    dag_id="reset_demo_state",
    start_date=datetime(2026, 8, 1),
    schedule=None,
    catchup=False,
    tags=["capstone", "maintenance"],
) as reset_dag:
    BashOperator(
        task_id="delete_injected_rows",
        bash_command=f"{PY} {SRC}/reset_demo.py",
        **COMMON,
    )
