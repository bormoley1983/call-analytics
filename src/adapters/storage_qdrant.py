# src/adapters/storage_qdrant.py
from __future__ import annotations

from typing import Any, Dict, List, Optional

from qdrant_client import QdrantClient
from qdrant_client.models import (Distance, FieldCondition, Filter, MatchValue,
                                  PointStruct, VectorParams)

COLLECTION = "call_transcripts"
VECTOR_DIM = 1024  # match your embedding model output


class QdrantStorage:
    """Semantic search over call transcripts using Qdrant."""

    def __init__(self, url: str = "http://localhost:6333"):
        self.client = QdrantClient(url=url)

    def ensure_collection(self) -> None:
        existing = [c.name for c in self.client.get_collections().collections]
        if COLLECTION not in existing:
            self.client.create_collection(
                collection_name=COLLECTION,
                vectors_config=VectorParams(size=VECTOR_DIM, distance=Distance.COSINE),
            )

    def upsert(self, call_id: str, embedding: List[float], payload: Dict[str, Any]) -> None:
        """Store a call transcript embedding with metadata payload."""
        point = PointStruct(
            id=abs(hash(call_id)) % (2**63),  # Qdrant requires uint64
            vector=embedding,
            payload={"call_id": call_id, **payload},
        )
        self.client.upsert(collection_name=COLLECTION, points=[point])

    def search(
        self,
        query_embedding: List[float],
        top_k: int = 10,
        filter_manager: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Find calls semantically similar to a query."""
        query_filter = None
        if filter_manager:
            query_filter = Filter(
                must=[FieldCondition(key="manager_id", match=MatchValue(value=filter_manager))]
            )
        results = self.client.query_points(
            collection_name=COLLECTION,
            query_vector=query_embedding,
            limit=top_k,
            query_filter=query_filter,
        ).points
        return [{"score": r.score, **(r.payload or {})} for r in results]