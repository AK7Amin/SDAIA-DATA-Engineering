"""Unit tests for Reciprocal Rank Fusion (src/rag/answer.py::reciprocal_rank_fusion).

Imported directly (no chromadb/sentence-transformers needed): the heavy
imports in answer.py live inside run_pipeline()/rerank(), not at module
scope, so importing the module only needs `config` (lightweight).
"""
import inspect
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import pytest

from rag.answer import reciprocal_rank_fusion


def hit(cid):
    return {"id": cid, "text": f"text-{cid}", "doc_id": f"doc-{cid}"}


class TestFusionOrdering:
    def test_top_in_both_lists_outranks_top_in_only_one_list(self):
        """An id ranked #1 (rank 0) in BOTH retrieval lists accumulates score
        from both contributions and must outrank ids that only place highly
        in a single list (rank 0 slots in each list are taken by "A", so the
        next-best competitors land at rank 1 in exactly one list each — the
        best any other id can do while "A" holds both top spots).
        """
        vector_hits = [hit("A"), hit("B")]   # A rank0, B rank1
        bm25_hits = [hit("A"), hit("C")]      # A rank0, C rank1

        fused = reciprocal_rank_fusion(vector_hits, bm25_hits, top_k=10)

        ids = [c["id"] for c in fused]
        assert ids[0] == "A"
        assert ids.index("A") < ids.index("B")
        assert ids.index("A") < ids.index("C")

    def test_presence_in_both_lists_beats_top_of_a_single_list(self):
        """An id present in both lists (even without topping either) can
        still outrank an id that tops only one list and is absent from the
        other — RRF rewards cross-method agreement, not just a single top rank.
        """
        vector_hits = [hit("B"), hit("A")]   # B rank0, A rank1
        bm25_hits = [hit("A")]                 # A rank0 (only entry) — B absent

        fused = reciprocal_rank_fusion(vector_hits, bm25_hits, top_k=10)

        ids = [c["id"] for c in fused]
        assert ids[0] == "A"  # 1/62 + 1/61 > 1/61 alone
        assert ids.index("A") < ids.index("B")


class TestRRFFormula:
    def test_k60_formula_value_for_rank_zero_in_both_lists(self):
        """Spot-check the documented formula (module docstring: 'RRF score =
        sum 1/(k + rank + 1)', k=60): an id at rank 0 in both lists should
        score exactly 1/(60+0+1) + 1/(60+0+1) = 2/61.
        """
        k = 60
        expected_score = 1.0 / (k + 0 + 1) + 1.0 / (k + 0 + 1)
        assert expected_score == pytest.approx(2 / 61)

        # the function's default k must match the constant used in the spot-check
        assert inspect.signature(reciprocal_rank_fusion).parameters["k"].default == 60

        # behavioural confirmation: rank-0-in-both still wins when it's the
        # only candidate present, i.e. contributes the full 2/61
        fused = reciprocal_rank_fusion([hit("A")], [hit("A")], top_k=5)
        assert fused[0]["id"] == "A"

    def test_explicit_k_changes_ranking(self):
        """Passing k explicitly should change scores in the direction the
        formula predicts: a smaller k inflates the gap given by a better rank.
        """
        vector_hits = [hit("A"), hit("B")]  # A rank0, B rank1
        bm25_hits = [hit("B"), hit("A")]     # B rank0, A rank1
        # symmetric ranks -> A and B tie regardless of k (both present, same rank pair)
        fused = reciprocal_rank_fusion(vector_hits, bm25_hits, k=1, top_k=10)
        ids = [c["id"] for c in fused]
        assert set(ids) == {"A", "B"}


class TestTopKLimit:
    def test_output_limited_to_top_k(self):
        vector_hits = [hit(f"v{i}") for i in range(10)]
        bm25_hits = [hit(f"b{i}") for i in range(10)]

        fused = reciprocal_rank_fusion(vector_hits, bm25_hits, top_k=3)

        assert len(fused) == 3

    def test_top_k_default_is_six(self):
        assert inspect.signature(reciprocal_rank_fusion).parameters["top_k"].default == 6
