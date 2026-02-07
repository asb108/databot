"""RAG (Retrieval-Augmented Generation) module using ChromaDB.

Provides semantic search over ingested documents, schemas, and past
conversations so the LLM can ground its answers in relevant context.
"""

from __future__ import annotations

import hashlib
import time
from typing import Any

from loguru import logger


class VectorStore:
    """ChromaDB-backed vector store with optional LiteLLM embeddings.

    Falls back to ChromaDB's default embedding function when LiteLLM
    embeddings are not configured.
    """

    def __init__(
        self,
        persist_directory: str = "",
        collection_name: str = "databot",
        embedding_model: str = "",
        api_key: str = "",
    ):
        self._persist_dir = persist_directory
        self._collection_name = collection_name
        self._embedding_model = embedding_model
        self._api_key = api_key
        self._client = None
        self._collection = None

    def _ensure_client(self) -> None:
        """Lazy-initialize ChromaDB client and collection."""
        if self._client is not None:
            return

        try:
            import chromadb
        except ImportError:
            raise ImportError("chromadb not installed. Install with: pip install databot[rag]")

        if self._persist_dir:
            self._client = chromadb.PersistentClient(path=self._persist_dir)
        else:
            self._client = chromadb.Client()

        embedding_fn = None
        if self._embedding_model:
            embedding_fn = self._build_embedding_function()

        kwargs: dict[str, Any] = {"name": self._collection_name}
        if embedding_fn:
            kwargs["embedding_function"] = embedding_fn

        self._collection = self._client.get_or_create_collection(**kwargs)
        logger.info(
            f"ChromaDB collection '{self._collection_name}' ready "
            f"({self._collection.count()} documents)"
        )

    def _build_embedding_function(self) -> Any:
        """Build a ChromaDB-compatible embedding function using LiteLLM."""
        model = self._embedding_model
        api_key = self._api_key

        class LiteLLMEmbeddingFunction:
            def __call__(self, input: list[str]) -> list[list[float]]:
                import litellm

                response = litellm.embedding(
                    model=model,
                    input=input,
                    api_key=api_key or None,
                )
                return [item["embedding"] for item in response.data]

        return LiteLLMEmbeddingFunction()

    def add(
        self,
        documents: list[str],
        metadatas: list[dict[str, Any]] | None = None,
        ids: list[str] | None = None,
    ) -> int:
        """Add documents to the vector store. Returns count added."""
        self._ensure_client()

        if not documents:
            return 0

        if ids is None:
            ids = [
                hashlib.sha256(f"{doc}:{i}".encode()).hexdigest()[:16]
                for i, doc in enumerate(documents)
            ]

        if metadatas is None:
            metadatas = [{"source": "manual", "added_at": time.time()} for _ in documents]

        self._collection.add(
            documents=documents,
            metadatas=metadatas,
            ids=ids,
        )
        return len(documents)

    def query(
        self,
        query_text: str,
        n_results: int = 5,
        where: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Query the vector store. Returns list of {document, metadata, distance}."""
        self._ensure_client()

        kwargs: dict[str, Any] = {
            "query_texts": [query_text],
            "n_results": n_results,
        }
        if where:
            kwargs["where"] = where

        results = self._collection.query(**kwargs)

        items = []
        docs = results.get("documents", [[]])[0]
        metas = results.get("metadatas", [[]])[0]
        distances = results.get("distances", [[]])[0]

        for doc, meta, dist in zip(docs, metas, distances):
            items.append(
                {
                    "document": doc,
                    "metadata": meta,
                    "distance": dist,
                }
            )

        return items

    def count(self) -> int:
        """Return the number of documents in the collection."""
        self._ensure_client()
        return self._collection.count()

    def delete(self, ids: list[str]) -> None:
        """Delete documents by ID."""
        self._ensure_client()
        self._collection.delete(ids=ids)

    def reset(self) -> None:
        """Delete and recreate the collection."""
        self._ensure_client()
        self._client.delete_collection(self._collection_name)
        self._collection = self._client.get_or_create_collection(name=self._collection_name)


class RAGContext:
    """Augments LLM prompts with relevant context from the vector store."""

    def __init__(
        self, store: VectorStore, max_context_docs: int = 5, max_context_chars: int = 4000
    ):
        self._store = store
        self._max_docs = max_context_docs
        self._max_chars = max_context_chars

    def enrich_prompt(self, user_message: str) -> str:
        """Query the vector store and return context to prepend to the prompt."""
        try:
            results = self._store.query(user_message, n_results=self._max_docs)
        except Exception as e:
            logger.warning(f"RAG query failed: {e}")
            return ""

        if not results:
            return ""

        context_parts = []
        total_chars = 0

        for item in results:
            doc = item["document"]
            if total_chars + len(doc) > self._max_chars:
                remaining = self._max_chars - total_chars
                if remaining > 100:
                    doc = doc[:remaining] + "..."
                else:
                    break
            source = item.get("metadata", {}).get("source", "unknown")
            context_parts.append(f"[Source: {source}]\n{doc}")
            total_chars += len(doc)

        if not context_parts:
            return ""

        return (
            "Relevant context from knowledge base:\n"
            "---\n" + "\n---\n".join(context_parts) + "\n---\n"
        )

    def ingest_schema(
        self, table_name: str, columns: list[dict[str, str]], database: str = ""
    ) -> None:
        """Ingest a table schema into the vector store for schema-aware queries."""
        cols_text = ", ".join(f"{c.get('name', '?')} ({c.get('type', '?')})" for c in columns)
        doc = f"Table: {table_name}\nDatabase: {database}\nColumns: {cols_text}"
        meta = {
            "source": "schema",
            "table": table_name,
            "database": database,
            "added_at": time.time(),
        }
        doc_id = hashlib.sha256(f"schema:{database}.{table_name}".encode()).hexdigest()[:16]
        self._store.add(documents=[doc], metadatas=[meta], ids=[doc_id])

    def ingest_conversation(self, session_key: str, user_msg: str, assistant_msg: str) -> None:
        """Ingest a conversation pair for long-term retrieval."""
        doc = f"User: {user_msg}\nAssistant: {assistant_msg}"
        meta = {
            "source": "conversation",
            "session": session_key,
            "added_at": time.time(),
        }
        doc_id = hashlib.sha256(f"conv:{session_key}:{user_msg[:50]}".encode()).hexdigest()[:16]
        self._store.add(documents=[doc], metadatas=[meta], ids=[doc_id])
