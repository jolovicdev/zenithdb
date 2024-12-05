import sqlite3
import json
import uuid
from typing import List, Dict, Any, Optional

BATCH_SIZE = 1000

class BulkOperations:
    """Bulk operations handler for SQLite."""
    
    def __init__(self, connection: sqlite3.Connection):
        """Initialize with an active database connection."""
        if not isinstance(connection, sqlite3.Connection):
            raise TypeError("Expected sqlite3.Connection object")
        self.connection = connection
        self._transaction_active = False
    
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
            for i in range(0, len(values), BATCH_SIZE):
                batch = values[i:i + BATCH_SIZE]
                cursor.executemany(
                    "INSERT INTO documents (id, collection, data) VALUES (?, ?, ?)",
                    batch
                )
            
            return doc_ids
        except Exception as e:
            if not self._transaction_active:
                self.connection.rollback()
            raise e
    
    def bulk_update(self, collection: str, updates: List[Dict[str, Any]]):
        """Update multiple documents in a single transaction."""
        cursor = self.connection.cursor()
        try:
            # Process updates in batches
            for i in range(0, len(updates), BATCH_SIZE):
                batch = updates[i:i + BATCH_SIZE]
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
        cursor = self.connection.cursor()
        try:
            # Delete in batches
            for i in range(0, len(doc_ids), BATCH_SIZE):
                batch = doc_ids[i:i + BATCH_SIZE]
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