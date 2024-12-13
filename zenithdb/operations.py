import sqlite3
import json
import uuid
from typing import List, Dict, Any, Optional, Callable

BATCH_SIZE = 1000

class BulkOperations:
    """Bulk operations handler for SQLite."""
    
    def __init__(self, connection: sqlite3.Connection):
        """Initialize with an active database connection."""
        if not isinstance(connection, sqlite3.Connection):
            raise TypeError("Expected sqlite3.Connection object")
        self.connection = connection
        self._transaction_active = False
        self.retry_count = 3
        self.progress_callback = None
    
    def set_progress_callback(self, callback: Callable[[int, int], None]):
        """Set callback for progress updates."""
        self.progress_callback = callback
    
    def _calculate_optimal_batch_size(self, documents: List[Dict[str, Any]]) -> int:
        """Calculate optimal batch size based on document size."""
        avg_doc_size = sum(len(str(doc)) for doc in documents[:100]) / min(100, len(documents))
        return min(BATCH_SIZE, max(100, int(1_000_000 / avg_doc_size)))
    
    def __enter__(self):
        """Start a transaction."""
        if not self._transaction_active:
            try:
                self.connection.execute("BEGIN IMMEDIATE")
                self._transaction_active = True
            except sqlite3.OperationalError as e:
                if "within a transaction" not in str(e):
                    raise
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Commit or rollback the transaction."""
        if exc_type is None and self._transaction_active:
            try:
                self.connection.commit()
            except Exception:
                self.connection.rollback()
                raise
        elif self._transaction_active:
            try:
                self.connection.rollback()
            except Exception:
                pass
        self._transaction_active = False
    
    def bulk_insert(self, collection: str, documents: List[Dict[str, Any]], 
                   doc_ids: Optional[List[str]] = None) -> List[str]:
        """Insert multiple documents in a single transaction."""
        if not documents:
            return []
        
        # Calculate optimal batch size based on document characteristics
        batch_size = self._calculate_optimal_batch_size(documents)
        
        if doc_ids is None:
            doc_ids = [str(uuid.uuid4()) for _ in documents]
        elif len(doc_ids) != len(documents):
            raise ValueError("Length of doc_ids must match length of documents")
        
        cursor = self.connection.cursor()
        try:
            # Add _id to each document
            for doc, doc_id in zip(documents, doc_ids):
                doc['_id'] = doc_id
            
            # Prepare documents for insertion
            values = [(doc_id, collection, json.dumps(doc)) 
                     for doc_id, doc in zip(doc_ids, documents)]
            
            # Insert in batches
            total = len(documents)
            for i in range(0, len(values), batch_size):
                batch = values[i:i + batch_size]
                cursor.executemany(
                    "INSERT INTO documents (id, collection, data) VALUES (?, ?, ?)",
                    batch
                )
                if self.progress_callback:
                    self.progress_callback(min(i + batch_size, total), total)
            
            return doc_ids
        except Exception as e:
            if not self._transaction_active:
                self.connection.rollback()
            raise e
    
    def bulk_update(self, collection: str, updates: List[Dict[str, Any]]):
        """Update multiple documents in a single transaction."""
        if not updates:
            return
        
        batch_size = self._calculate_optimal_batch_size(updates)
        cursor = self.connection.cursor()
        try:
            # Process updates in batches
            for i in range(0, len(updates), batch_size):
                batch = updates[i:i + batch_size]
                for update in batch:
                    doc_id = update.pop('_id', None)
                    if doc_id:
                        cursor.execute(
                            """UPDATE documents 
                               SET data = json_patch(data, ?),
                                   updated_at = CURRENT_TIMESTAMP 
                               WHERE id = ? AND collection = ?""",
                            (json.dumps(update), doc_id, collection)
                        )
            
            if not self._transaction_active:
                self.connection.commit()
        except Exception as e:
            if not self._transaction_active:
                self.connection.rollback()
            raise e
    
    def bulk_delete(self, collection: str, doc_ids: List[str]):
        """Delete multiple documents in a single transaction."""
        if not doc_ids:
            return
        
        # For deletes, use a simpler calculation since we're just dealing with IDs
        batch_size = min(BATCH_SIZE, max(100, len(doc_ids) // 10))
        cursor = self.connection.cursor()
        try:
            # Delete in batches
            for i in range(0, len(doc_ids), batch_size):
                batch = doc_ids[i:i + batch_size]
                placeholders = ','.join(['?' for _ in batch])
                cursor.execute(
                    f"DELETE FROM documents WHERE id IN ({placeholders}) AND collection = ?",
                    batch + [collection]
                )
            
            if not self._transaction_active:
                self.connection.commit()
        except Exception as e:
            if not self._transaction_active:
                self.connection.rollback()
            raise e