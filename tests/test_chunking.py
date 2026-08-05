"""Unit tests for sentence-level chunking (src/rag/build_index.py::chunk_documents).

Imported directly (no chromadb/sentence-transformers needed): the heavy
imports in build_index.py live inside build_chroma_collection()/build_bm25_index(),
not at module scope, so importing the module only needs `config` (lightweight).
"""
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from rag.build_index import chunk_documents

FIVE_SENTENCE_TEXT = "One. Two. Three. Four. Five."


def make_doc(doc_id="doc1", text=FIVE_SENTENCE_TEXT):
    return {"doc_id": doc_id, "text": text}


class TestChunkDocuments:
    def test_multi_sentence_doc_yields_more_than_one_chunk(self):
        chunks = chunk_documents([make_doc()], chunk_size=2)
        assert len(chunks) > 1

    def test_chunks_overlap_by_one_sentence(self):
        chunks = chunk_documents([make_doc()], chunk_size=2)
        # every consecutive pair of chunks should share at least one sentence:
        # the last sentence of chunk N is the first sentence of chunk N+1.
        # chunks carry a "[title] " contextual header — strip it before comparing.
        header = re.compile(r"^\[[^\]]*\]\s*")
        for prev, nxt in zip(chunks, chunks[1:]):
            prev_sentences = re.split(r"(?<=[.!?])\s+", header.sub("", prev["text"]).strip())
            next_sentences = re.split(r"(?<=[.!?])\s+", header.sub("", nxt["text"]).strip())
            assert prev_sentences[-1] == next_sentences[0], (
                f"expected overlap between {prev['text']!r} and {nxt['text']!r}"
            )

    def test_chunk_ids_follow_naming_convention(self):
        chunks = chunk_documents([make_doc(doc_id="doc1")], chunk_size=2)
        pattern = re.compile(r"^doc1_chunk_\d{3}$")
        for c in chunks:
            assert pattern.match(c["id"]), f"unexpected chunk id: {c['id']}"

    def test_every_chunk_is_non_empty(self):
        chunks = chunk_documents([make_doc()], chunk_size=2)
        assert len(chunks) > 0
        for c in chunks:
            assert c["text"].strip() != ""

    def test_chunk_doc_id_preserved(self):
        chunks = chunk_documents([make_doc(doc_id="doc42")], chunk_size=2)
        assert all(c["doc_id"] == "doc42" for c in chunks)

    def test_single_sentence_doc_yields_one_chunk(self):
        chunks = chunk_documents([make_doc(text="Just one sentence.")], chunk_size=2)
        assert len(chunks) == 1
        # text carries the contextual header, then the sentence itself
        assert chunks[0]["text"].endswith("Just one sentence.")
        assert chunks[0]["text"].startswith("[")
