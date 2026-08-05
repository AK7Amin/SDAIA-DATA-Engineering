"""Builds the hybrid retrieval index consumed by answer.py (rubric: RAG pipeline).

Reuses the Day3_Lab patterns exactly:
  - chunk_documents(): sentence-level chunks, chunk_size=2, 1-sentence overlap
  - a persistent ChromaDB collection embedded with all-MiniLM-L6-v2
  - a BM25Okapi keyword index, pickled together with the chunk list

Idempotent: the ChromaDB collection is deleted and recreated every run, so
re-running never .add()s duplicate ids onto a stale collection.
"""
import json
import os
import pickle
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import CHROMA_DIR, DATA_DIR  # noqa: E402
from lineage import stage  # noqa: E402

COLLECTION_NAME = "rag_capstone_chunks"
DOCS_PATH = os.path.join(DATA_DIR, "rag_docs.jsonl")
BM25_PATH = os.path.join(DATA_DIR, "bm25.pkl")


def load_documents(path=DOCS_PATH):
    docs = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                docs.append(json.loads(line))
    return docs


def chunk_documents(docs, chunk_size=2):
    """Sentence-level chunking with 1-sentence overlap — identical to Day3_Lab."""
    all_chunks = []
    for doc in docs:
        sentences = re.split(r"(?<=[.!?])\s+", doc["text"].strip())
        # contextual header: without it, a mid-document chunk like "211 units
        # were sold" loses the product's identity and retrieval goes blind
        header = doc.get("title") or doc["doc_id"]
        for i in range(0, len(sentences), max(1, chunk_size - 1)):
            chunk_text = " ".join(sentences[i : i + chunk_size])
            if not chunk_text.strip():
                continue
            all_chunks.append(
                {
                    "id": f"{doc['doc_id']}_chunk_{i:03d}",
                    "text": f"[{header}] {chunk_text}",
                    "doc_id": doc["doc_id"],
                }
            )
    return all_chunks


def build_chroma_collection(chunks):
    import chromadb
    from chromadb.utils.embedding_functions import SentenceTransformerEmbeddingFunction

    os.makedirs(CHROMA_DIR, exist_ok=True)
    client = chromadb.PersistentClient(path=CHROMA_DIR)
    try:
        client.delete_collection(COLLECTION_NAME)
    except Exception:
        pass  # collection did not exist yet on a fresh CHROMA_DIR — fine

    ef = SentenceTransformerEmbeddingFunction(model_name="all-MiniLM-L6-v2")
    collection = client.create_collection(COLLECTION_NAME, embedding_function=ef)
    # ChromaDB caps a single add() at ~5461 items — feed it in batches
    BATCH = 5000
    for start in range(0, len(chunks), BATCH):
        batch = chunks[start:start + BATCH]
        collection.add(
            ids=[c["id"] for c in batch],
            documents=[c["text"] for c in batch],
            metadatas=[{"doc_id": c["doc_id"]} for c in batch],
        )
        print(f"[INDEX] embedded+stored chunks {start}..{start + len(batch) - 1}")
    return collection


def build_bm25_index(chunks):
    from rank_bm25 import BM25Okapi

    tokenized = [c["text"].lower().split() for c in chunks]
    bm25 = BM25Okapi(tokenized)
    with open(BM25_PATH, "wb") as f:
        pickle.dump({"bm25": bm25, "chunks": chunks}, f)
    return bm25


def main():
    docs = load_documents()
    chunks = chunk_documents(docs, chunk_size=2)
    print(f"[build_index] {len(docs)} documents -> {len(chunks)} chunks")

    if docs:
        example = docs[0]
        n_chunks = sum(1 for c in chunks if c["doc_id"] == example["doc_id"])
        print(
            f"[build_index] example doc '{example['doc_id']}' split into "
            f"{n_chunks} chunks (multi-sentence source -> multiple chunks)"
        )

    build_chroma_collection(chunks)
    print(
        f"[build_index] Chroma collection '{COLLECTION_NAME}' rebuilt at "
        f"{CHROMA_DIR} ({len(chunks)} chunks, all-MiniLM-L6-v2 embeddings)"
    )

    build_bm25_index(chunks)
    print(f"[build_index] BM25 index pickled -> {BM25_PATH}")


if __name__ == "__main__":
    with stage("build_rag_index"):
        main()
