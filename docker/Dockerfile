# Airflow + Spark/Delta + RAG stack — runs the whole capstone pipeline on Linux
# (avoids every Windows/winutils pitfall; JDK 17 required by Spark)
FROM apache/airflow:2.10.5-python3.12

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && apt-get clean && rm -rf /var/lib/apt/lists/* \
    && install -d -o airflow -g root /opt/venvs
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
# Heavy compute deps (torch/chromadb/pyspark/GX) live in a SEPARATE venv so
# Airflow's tightly-pinned environment stays untouched and image rebuilds stay
# fast. DAG tasks run /opt/venvs/compute/bin/python <script> via BashOperator.
# torch is installed alone from the CPU-only index first (default PyPI serves
# multi-GB CUDA wheels); the second install then finds torch already satisfied.
RUN python -m venv /opt/venvs/compute \
    && /opt/venvs/compute/bin/pip install --no-cache-dir --upgrade pip \
    && /opt/venvs/compute/bin/pip install --no-cache-dir \
        --index-url https://download.pytorch.org/whl/cpu torch \
    && /opt/venvs/compute/bin/pip install --no-cache-dir \
        pyspark==3.5.9 \
        delta-spark==3.2.0 \
        kafka-python \
        pandas pyarrow openpyxl \
        "pydantic>=2" \
        great_expectations==1.19.1 \
        openlineage-python \
        chromadb \
        sentence-transformers \
        rank_bm25 \
        loguru
# Spark workers must use the compute venv's python, not Airflow's
ENV PYSPARK_PYTHON=/opt/venvs/compute/bin/python \
    PYSPARK_DRIVER_PYTHON=/opt/venvs/compute/bin/python
