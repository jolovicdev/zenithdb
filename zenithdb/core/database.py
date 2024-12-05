import sqlite3
import json
from typing import Dict, List, Any, Union, Optional, Tuple
from .connection_pool import ConnectionPool
from .collection import Collection
from ..query import Query, QueryOperator
from ..operations import BulkOperations
from ..aggregations import Aggregations, AggregateFunction
import functools
import hashlib

class Database:
    """NoSQL-like database interface using SQLite as backend."""
    
    def __init__(self, db_path: str, max_connections: int = 10, max_result_size: int = 10000):
        """Initialize database with connection pool."""
        self.db_path = db_path
        self.pool = ConnectionPool(db_path, max_connections)
        self.max_result_size = max_result_size
        self._init_db()
    
    def collection(self, name: str) -> Collection:
        """Get a collection interface."""
        return Collection(self, name)
    
    def bulk_operations(self) -> BulkOperations:
        """Get bulk operations interface."""
        with self.pool.get_connection() as conn:
            return BulkOperations(conn)
    
    def _init_db(self):
        """Initialize database with optimized settings."""
        with self.pool.get_connection() as conn:
            conn.executescript('''
                PRAGMA journal_mode=WAL;
                PRAGMA synchronous=NORMAL;
                PRAGMA temp_store=MEMORY;
                PRAGMA page_size=4096;
                PRAGMA cache_size=-32000;  -- 128MB cache
                PRAGMA auto_vacuum=NONE;
                PRAGMA locking_mode=EXCLUSIVE;
                PRAGMA busy_timeout=5000;
                PRAGMA mmap_size=536870912;  -- 512MB memory mapping
                PRAGMA journal_size_limit=67108864;  -- 64MB journal size limit
                PRAGMA threads=4;  -- Use multiple threads
            ''')
            
            conn.executescript('''
                CREATE TABLE IF NOT EXISTS documents (
                    id TEXT PRIMARY KEY,
                    collection TEXT NOT NULL,
                    data TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE INDEX IF NOT EXISTS idx_collection ON documents(collection) WHERE collection IS NOT NULL;
                CREATE INDEX IF NOT EXISTS idx_collection_id ON documents(collection, id) WHERE collection IS NOT NULL;
                
                CREATE TABLE IF NOT EXISTS indexes (
                    name TEXT PRIMARY KEY,
                    collection TEXT NOT NULL,
                    fields TEXT NOT NULL,
                    type TEXT NOT NULL,
                    unique_index INTEGER NOT NULL
                );
            ''')
    
    def create_index(self, collection: str, fields: Union[str, List[str]], 
                    index_type: str = "btree", unique: bool = False) -> str:
        """Create an index on specified fields."""
        if isinstance(fields, str):
            fields = [fields]
            
        # Generate index name
        safe_fields = [field.replace('.', '_') for field in fields]
        index_name = f"idx_{collection}_{'_'.join(safe_fields)}"
        
        with self.pool.get_connection() as conn:
            # Store index metadata
            conn.execute(
                "INSERT OR REPLACE INTO indexes VALUES (?, ?, ?, ?, ?)",
                (index_name, collection, json.dumps(fields), index_type, int(unique))
            )
            
            # Create the actual index
            field_exprs = []
            for field in fields:
                if '.' in field:
                    parts = field.split('.')
                    expr = f"json_extract(data, '$.{'.'.join(parts)}')"
                else:
                    expr = f"json_extract(data, '$.{field}')"
                field_exprs.append(expr)
            
            index_sql = f"""
                CREATE {'UNIQUE' if unique else ''} INDEX IF NOT EXISTS {index_name}
                ON documents({', '.join(['collection'] + field_exprs)})
                WHERE collection = '{collection}'
            """
            conn.execute(index_sql)
            conn.commit()
        
        return index_name
    
    def list_indexes(self, collection: str = None) -> List[Dict[str, Any]]:
        """List all indexes or indexes for a specific collection."""
        with self.pool.get_connection() as conn:
            cursor = conn.cursor()
            if collection:
                cursor.execute("SELECT * FROM indexes WHERE collection = ?", [collection])
            else:
                cursor.execute("SELECT * FROM indexes")
            
            return [{
                'name': row[0],
                'collection': row[1],
                'fields': json.loads(row[2]),
                'type': row[3],
                'unique': bool(row[4])
            } for row in cursor]
    
    def drop_index(self, index_name: str):
        """Drop an index by name."""
        with self.pool.get_connection() as conn:
            conn.execute(f"DROP INDEX IF EXISTS {index_name}")
            conn.execute("DELETE FROM indexes WHERE name = ?", [index_name])
            conn.commit()
    
    def insert(self, collection: str, document: Dict[str, Any], doc_id: Optional[str] = None) -> str:
        """Insert a document into a collection."""
        with self.pool.get_connection() as conn:
            ops = BulkOperations(conn)
            return ops.bulk_insert(collection, [document], [doc_id] if doc_id else None)[0]
    
    def execute_query(self, query: 'Query') -> List[Dict[str, Any]]:
        """Execute a query and return results with optimized execution."""
        conditions = []
        params = []
        
        for field, op, value in query.conditions:
            if value is None:
                conditions.append(f"(json_extract(data, '$.{field}') IS NULL AND json_type(data, '$.{field}') IS NOT NULL)")
                continue
                
            if op == QueryOperator.CONTAINS:
                conditions.append(f"json_extract(data, '$.{field}') LIKE ?")
                params.append(f"%{value}%")
            elif op == QueryOperator.EQ:
                if '.' in field:
                    conditions.append(f"json_extract(data, '$.{field}') = ?")
                else:
                    conditions.append(f"json_extract(data, '$.{field}') = ?")
                params.append(value)
            else:
                op_map = {
                    QueryOperator.GT: ">",
                    QueryOperator.GTE: ">=",
                    QueryOperator.LT: "<",
                    QueryOperator.LTE: "<=",
                    QueryOperator.NE: "!=",
                }
                conditions.append(f"json_extract(data, '$.{field}') {op_map[op]} ?")
                params.append(value)
        
        where_clause = " AND ".join(conditions) if conditions else "1"
        
        # Add pagination if not specified
        if query.limit_value is None:
            query.limit_value = self.max_result_size
        if query.skip_value is None:
            query.skip_value = 0
            
        # Use optimized query with MATERIALIZED optimization
        sql = f"""
            WITH RECURSIVE
            json_data AS MATERIALIZED (
                SELECT data 
                FROM documents
                WHERE collection = ? AND {where_clause}
                LIMIT ? OFFSET ?
            )
            SELECT * FROM json_data
        """
        
        all_params = [query.collection] + params + [query.limit_value, query.skip_value]
        
        with self.pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA read_uncommitted = ON")  # Allow dirty reads for better concurrency
            cursor.execute(sql, all_params)
            
            # Use optimized batch processing with larger batch size
            return self._process_results(cursor, batch_size=10000)
    
    def _process_results(self, cursor: sqlite3.Cursor, batch_size: int = 10000) -> List[Dict[str, Any]]:
        """Process results in efficient batches with minimal memory overhead."""
        results = []
        while True:
            batch = cursor.fetchmany(batch_size)
            if not batch:
                break
            # Use list comprehension for better performance
            results.extend([json.loads(row[0]) for row in batch])
        return results
    
    def _get_index_hint(self, collection: str, conditions: List[Tuple[str, QueryOperator, Any]]) -> str:
        """Get the best index hint for the query."""
        if not conditions:
            return ""
            
        # Check available indexes
        indexes = self.list_indexes(collection)
        if not indexes:
            return ""
            
        # Find the most suitable index based on query conditions
        for index in indexes:
            fields = json.loads(index['fields']) if isinstance(index['fields'], str) else index['fields']
            if isinstance(fields, str):
                fields = [fields]
            
            # Check if the first condition matches any indexed field
            first_condition = conditions[0]
            field_name = first_condition[0]
            
            # Skip CONTAINS operations for index hints as they use LIKE
            if first_condition[1] == QueryOperator.CONTAINS:
                continue
                
            if field_name in fields or any(field.startswith(f"{field_name}.") for field in fields):
                # Verify the index exists in SQLite
                with self.pool.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name=?", (index['name'],))
                    if cursor.fetchone():
                        return f"INDEXED BY {index['name']}"
        
        return ""
    
    def update(self, collection: str, query: Dict[str, Any], update: Dict[str, Any]) -> int:
        """Update documents matching query."""
        with self.pool.get_connection() as conn:
            cursor = conn.cursor()
            
            # Build WHERE clause
            where_conditions = []
            params = []
            for field, value in query.items():
                if field == "_id":
                    where_conditions.append("id = ?")
                    params.append(value)
                elif isinstance(value, dict):
                    for op, val in value.items():
                        op = op.lstrip("$")
                        if op in ("gt", "lt", "gte", "lte", "ne"):
                            op_map = {"gt": ">", "lt": "<", "gte": ">=", "lte": "<=", "ne": "!="}
                            where_conditions.append(f"json_extract(data, '$.{field}') {op_map[op]} ?")
                            params.append(json.dumps(val) if isinstance(val, str) else val)
                        elif op == "in":
                            if '.' in field:
                                # Handle nested fields
                                placeholders = ','.join(['?' for _ in val])
                                where_conditions.append(f"json_extract(data, '$.{field}') IN ({placeholders})")
                                params.extend([json.dumps(v) if isinstance(v, str) else v for v in val])
                            else:
                                # Handle array fields
                                where_conditions.append(f"json_extract(data, '$.{field}') LIKE ?")
                                params.append(f'%{json.dumps(val[0])[1:-1]}%')
                        elif op == "contains":
                            where_conditions.append(f"json_extract(data, '$.{field}') LIKE ?")
                            params.append(f'%{json.dumps(val)[1:-1]}%')
                else:
                    if '.' in field:
                        # Handle nested fields
                        where_conditions.append(f"json_extract(data, '$.{field}') = ?")
                        params.append(json.dumps(value) if isinstance(value, str) else value)
                    else:
                        # Handle array fields and regular fields
                        where_conditions.append(f"json_extract(data, '$.{field}') = ?")
                        params.append(json.dumps(value) if isinstance(value, str) else value)
            
            # Add collection condition
            where_conditions.append("collection = ?")
            params.append(collection)
            
            # Get matching documents
            cursor.execute(
                f"SELECT id, data FROM documents WHERE {' AND '.join(where_conditions)}",
                params
            )
            
            # Update documents
            updated = 0
            for doc_id, doc_data in cursor:
                doc = json.loads(doc_data)
                if "$set" in update:
                    for field, value in update["$set"].items():
                        parts = field.split('.')
                        current = doc
                        for part in parts[:-1]:
                            if part not in current:
                                current[part] = {}
                            current = current[part]
                        
                        # Handle array index updates
                        last_part = parts[-1]
                        if current and isinstance(current, list) and last_part.isdigit():
                            current[int(last_part)] = value
                        else:
                            current[last_part] = value
                else:
                    doc.update(update)
                
                cursor.execute(
                    "UPDATE documents SET data = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                    (json.dumps(doc), doc_id)
                )
                updated += cursor.rowcount
            
            conn.commit()
            return updated
    
    def delete(self, collection: str, query: Dict[str, Any]) -> int:
        """Delete documents matching query."""
        with self.pool.get_connection() as conn:
            cursor = conn.cursor()
            
            # Build WHERE clause
            where_conditions = []
            params = []
            for field, value in query.items():
                if field == "_id":
                    where_conditions.append("id = ?")
                    params.append(value)
                elif isinstance(value, dict):
                    for op, val in value.items():
                        op = op.lstrip("$")
                        if op in ("gt", "lt", "gte", "lte", "ne"):
                            op_map = {"gt": ">", "lt": "<", "gte": ">=", "lte": "<=", "ne": "!="}
                            where_conditions.append(f"json_extract(data, '$.{field}') {op_map[op]} ?")
                            params.append(json.dumps(val) if isinstance(val, str) else val)
                        elif op == "in":
                            placeholders = ','.join(['?' for _ in val])
                            where_conditions.append(f"json_extract(data, '$.{field}') IN ({placeholders})")
                            params.extend([json.dumps(v) if isinstance(v, str) else v for v in val])
                        elif op == "contains":
                            where_conditions.append(f"json_extract(data, '$.{field}') LIKE ?")
                            params.append(f'%{json.dumps(val)[1:-1]}%')
                else:
                    if '.' in field:
                        # Handle nested fields
                        where_conditions.append(f"json_extract(data, '$.{field}') = ?")
                        params.append(json.dumps(value) if isinstance(value, str) else value)
                    else:
                        # Handle regular fields
                        where_conditions.append(f"json_extract(data, '$.{field}') = ?")
                        params.append(json.dumps(value) if isinstance(value, str) else value)
            
            # Add collection condition
            where_conditions.append("collection = ?")
            params.append(collection)
            
            # Execute delete
            cursor.execute(
                f"DELETE FROM documents WHERE {' AND '.join(where_conditions)}",
                params
            )
            
            deleted = cursor.rowcount
            conn.commit()
            return deleted
    
    def _check_connection_health(self, conn: sqlite3.Connection) -> bool:
        """Check if connection is healthy."""
        try:
            conn.execute("SELECT 1").fetchone()
            return True
        except sqlite3.Error:
            return False
    
    def close(self):
        """Close all database connections."""
        # Close connections in the same thread they were created
        with self.pool.get_connection() as conn:
            conn.close()
        self.pool.close_all()