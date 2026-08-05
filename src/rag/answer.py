"""CLI answer engine for the capstone RAG pipeline (rubric: grounded, cited answers).

Retrieval: ChromaDB vector search (top 6) + BM25 keyword search (top 6) ->
Reciprocal Rank Fusion (k=60, top 6) -> cross-encoder rerank (top 3).
The final answer is extractive (no LLM call): top-3 chunk text is stitched
with [Source N] citations, followed by a source list mapping each citation
back to its chunk id, doc id, and chunk text.

Usage: python answer.py "question here" [--compare]
"""
import argparse
import os
import pickle
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import CHROMA_DIR, DATA_DIR  # noqa: E402

COLLECTION_NAME = "rag_capstone_chunks"
BM25_PATH = os.path.join(DATA_DIR, "bm25.pkl")
RRF_K = 60


def load_bm25():
    with open(BM25_PATH, "rb") as f:
        data = pickle.load(f)
    return data["bm25"], data["chunks"]


def vector_search(collection, query, top_k=6):
    res = collection.query(query_texts=[query], n_results=top_k)
    return [
        {"id": cid, "text": doc, "doc_id": meta["doc_id"]}
        for cid, doc, meta in zip(res["ids"][0], res["documents"][0], res["metadatas"][0])
    ]


def bm25_search(bm25, chunks, query, top_k=6):
    scores = bm25.get_scores(query.lower().split())
    ranked = sorted(enumerate(scores), key=lambda x: x[1], reverse=True)
    return [chunks[idx] for idx, _ in ranked[:top_k]]


def reciprocal_rank_fusion(vector_hits, bm25_hits, k=RRF_K, top_k=6):
    """RRF score = sum 1/(k + rank + 1) across both lists — same k=60 as Day3_Lab."""
    scores = {}
    id_to_chunk = {}
    for rank, hit in enumerate(vector_hits):
        scores[hit["id"]] = scores.get(hit["id"], 0.0) + 1.0 / (k + rank + 1)
        id_to_chunk[hit["id"]] = hit
    for rank, hit in enumerate(bm25_hits):
        scores[hit["id"]] = scores.get(hit["id"], 0.0) + 1.0 / (k + rank + 1)
        id_to_chunk[hit["id"]] = hit
    sorted_ids = sorted(scores, key=lambda x: scores[x], reverse=True)
    return [id_to_chunk[cid] for cid in sorted_ids[:top_k]]


def rerank(query, candidates, top_k=3):
    from sentence_transformers import CrossEncoder

    model = CrossEncoder("cross-encoder/ms-marco-MiniLM-L-6-v2")
    pairs = [(query, c["text"]) for c in candidates]
    scores = model.predict(pairs)
    ranked = sorted(zip(scores, candidates), key=lambda x: x[0], reverse=True)
    return [c for _, c in ranked[:top_k]]


def compose_answer(query, top_chunks):
    """Extractive answer: retrieved chunk text stitched with [Source N] citations."""
    body = " ".join(f"{c['text']} [Source {i + 1}]" for i, c in enumerate(top_chunks))
    lines = [f"Question: {query}", "", "Answer:", body, "", "Sources:"]
    for i, c in enumerate(top_chunks):
        lines.append(
            f'  [Source {i + 1}] chunk_id={c["id"]} doc_id={c["doc_id"]} text="{c["text"]}"'
        )
    return "\n".join(lines)


def expand_query(query):
    """Pre-retrieval query rewriting (Day 3, Advanced RAG pattern).

    Superlative intent ("top/best product") fails against this corpus because
    'top' and 'revenue' occur in every document ('top countries', 'total
    revenue'), so neither BM25 nor the embedding can tell rank 1 from rank
    423. The rank-1/top-10 documents state their status in distinctive words —
    expanding the query with those same words closes the vocabulary gap.
    """
    ql = query.lower()
    if any(t in ql for t in ("top ", "best", "highest", "number one", "#1")):
        expanded = (query + " (the number one best-selling highest-revenue "
                    "product, ranks 1 by total revenue)")
        print(f"[answer] query expanded for retrieval: {expanded}")
        return expanded
    return query


def run_pipeline(query):
    import chromadb
    from chromadb.utils.embedding_functions import SentenceTransformerEmbeddingFunction

    ef = SentenceTransformerEmbeddingFunction(model_name="all-MiniLM-L6-v2")
    client = chromadb.PersistentClient(path=CHROMA_DIR)
    collection = client.get_collection(COLLECTION_NAME, embedding_function=ef)
    bm25, chunks = load_bm25()
    query = expand_query(query)

    # wide-then-precise: catalog docs share most vocabulary ("revenue",
    # "invoices" appear in all 662), so stage-1 recall needs depth — the
    # cross-encoder then reads (query, chunk) jointly and lifts the right one
    vec_hits = vector_search(collection, query, top_k=15)
    bm25_hits = bm25_search(bm25, chunks, query, top_k=15)
    fused = reciprocal_rank_fusion(vec_hits, bm25_hits, top_k=12)
    reranked = rerank(query, fused, top_k=3)
    return vec_hits, bm25_hits, fused, reranked


def print_comparison(vec_hits, bm25_hits, fused, reranked):
    """Prints top-3 ids side by side — evidence each stage changes the ranking."""
    cols = [[c["id"] for c in lst[:3]] for lst in (bm25_hits, vec_hits, fused, reranked)]
    print("".join(f"{h:<22}" for h in ("BM25", "Vector", "RRF", "Rerank")))
    for row_idx in range(max(len(c) for c in cols)):
        cells = [col[row_idx] if row_idx < len(col) else "-" for col in cols]
        print("".join(f"{cell:<22}" for cell in cells))


def main():
    parser = argparse.ArgumentParser(description="Extractive RAG answer over the capstone catalog")
    parser.add_argument("question")
    parser.add_argument("--compare", action="store_true", help="show per-stage top-3 ids")
    args = parser.parse_args()

    vec_hits, bm25_hits, fused, reranked = run_pipeline(args.question)

    if args.compare:
        print("\n[answer] stage-by-stage top-3 ids (each stage re-ranks the candidates):")
        print_comparison(vec_hits, bm25_hits, fused, reranked)
        print()

    print(compose_answer(args.question, reranked))


if __name__ == "__main__":
    main()
