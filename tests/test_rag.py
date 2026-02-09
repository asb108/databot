"""Tests for the RAG / vector memory module."""

from __future__ import annotations

import pytest

from databot.rag import RAGContext, VectorStore

# ---------------------------------------------------------------------------
# VectorStore (in-memory ChromaDB)
# ---------------------------------------------------------------------------


class TestVectorStore:
    @pytest.fixture
    def store(self):
        """In-memory ChromaDB store."""
        try:
            import chromadb
        except ImportError:
            pytest.skip("chromadb not installed")
        return VectorStore(collection_name="test_collection")

    def test_add_and_count(self, store):
        count = store.add(["document one", "document two"])
        assert count == 2
        assert store.count() == 2

    def test_query(self, store):
        store.add(
            documents=["Python is a programming language", "Java is a programming language"],
            ids=["doc1", "doc2"],
        )
        results = store.query("Python", n_results=1)
        assert len(results) == 1
        assert "Python" in results[0]["document"]

    def test_delete(self, store):
        store.add(documents=["to delete"], ids=["del1"])
        assert store.count() == 1
        store.delete(ids=["del1"])
        assert store.count() == 0

    def test_reset(self, store):
        store.add(["one", "two", "three"])
        assert store.count() == 3
        store.reset()
        assert store.count() == 0

    def test_query_empty_store(self, store):
        results = store.query("anything")
        assert results == []


# ---------------------------------------------------------------------------
# RAGContext
# ---------------------------------------------------------------------------


class TestRAGContext:
    @pytest.fixture
    def rag(self):
        try:
            import chromadb
        except ImportError:
            pytest.skip("chromadb not installed")
        store = VectorStore(collection_name="test_rag")
        store.add(
            documents=[
                "Table: users, Columns: id (int), name (varchar), email (varchar)",
                "Table: orders, Columns: id (int), user_id (int), total (decimal)",
                "Airflow DAG etl_pipeline runs daily at midnight",
            ],
            metadatas=[
                {"source": "schema", "table": "users"},
                {"source": "schema", "table": "orders"},
                {"source": "docs", "topic": "airflow"},
            ],
            ids=["s1", "s2", "d1"],
        )
        return RAGContext(store, max_context_docs=3)

    def test_enrich_prompt_relevant(self, rag):
        context = rag.enrich_prompt("show me user data")
        assert "users" in context.lower() or "knowledge base" in context.lower()
        assert len(context) > 0

    def test_enrich_prompt_empty_query(self, rag):
        # Should still return something
        context = rag.enrich_prompt("")
        # ChromaDB may or may not return results for empty query
        assert isinstance(context, str)

    def test_ingest_schema(self):
        try:
            import chromadb
        except ImportError:
            pytest.skip("chromadb not installed")

        store = VectorStore(collection_name="test_ingest")
        rag = RAGContext(store)
        rag.ingest_schema(
            "customers",
            [{"name": "id", "type": "int"}, {"name": "name", "type": "varchar"}],
            database="analytics",
        )
        assert store.count() == 1
        results = store.query("customers")
        assert len(results) == 1
        assert "customers" in results[0]["document"]

    def test_ingest_conversation(self):
        try:
            import chromadb
        except ImportError:
            pytest.skip("chromadb not installed")

        store = VectorStore(collection_name="test_conv")
        rag = RAGContext(store)
        rag.ingest_conversation(
            "cli:test",
            "How many users do we have?",
            "There are 1,234 users in the database.",
        )
        assert store.count() == 1

    def test_max_context_chars(self):
        try:
            import chromadb
        except ImportError:
            pytest.skip("chromadb not installed")

        store = VectorStore(collection_name="test_limit")
        # Add a long document
        long_doc = "x" * 5000
        store.add([long_doc], ids=["long1"])
        rag = RAGContext(store, max_context_chars=200)
        context = rag.enrich_prompt("test")
        # Context should be truncated
        assert len(context) < 5000
