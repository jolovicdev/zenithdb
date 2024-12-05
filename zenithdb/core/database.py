import sqlite3
import json
from typing import Dict, List, Any, Union, Optional
from .connection_pool import ConnectionPool
from .collection import Collection
from ..query import Query, QueryOperator
from ..operations import BulkOperations
from ..aggregations import Aggregations, AggregateFunction

class Database:
    """NoSQL-like database interface using SQLite as backend."""
    
    def __init__(self, db_path: str, max_connections: int = 10):
        """Initialize database with connection pool."""
        self.db_path = db_path
        self.pool = ConnectionPool(db_path, max_connections)
        self._init_db()
    
    def collection(self, name: str) -> Collection:
        """Get a collection interface."""
        return Collection(self, name)
    
    def bulk_operations(self) -> BulkOperations:
        """Get bulk operations interface."""
        with self.pool.get_connection() as conn:
            return BulkOperations(conn)
    
    def _init_db(self):
        """Initialize database with optimized settings and schema."""
        with self.pool.get_connection() as conn:
            # Performance optimizations
            conn.executescript('''
                PRAGMA journal_mode=WAL;
                PRAGMA synchronous=NORMAL;
                PRAGMA temp_store=MEMORY;
                PRAGMA page_size=4096;
                PRAGMA cache_size=-16000;
                PRAGMA auto_vacuum=NONE;
                PRAGMA locking_mode=EXCLUSIVE;
                PRAGMA busy_timeout=5000;
            ''')
            
            # Create tables with optimized schema
            conn.executescript('''
                CREATE TABLE IF NOT EXISTS documents (
                    id TEXT PRIMARY KEY,
                    collection TEXT NOT NULL,
                    data TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE INDEX IF NOT EXISTS idx_collection ON documents(collection);
                CREATE INDEX IF NOT EXISTS idx_collection_id ON documents(collection, id);
                
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
                ON documents({', '.join(field_exprs)})
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
    
    def execute_query(self, query: Query) -> List[Dict[str, Any]]:
        """Execute a query and return matching documents."""
        with self.pool.get_connection() as conn:
            cursor = conn.cursor()
            
            sql_parts = ["SELECT id, data FROM documents"]
            params = []
            
            where_conditions = []
            for field, operator, value in query.conditions:
                if field == "_id":
                    where_conditions.append("id = ?")
                    params.append(value)
                elif field == "user_id":  # Special handling for relationships
                    where_conditions.append(f"json_extract(data, '$.{field}') = ?")
                    params.append(value)  # Don't JSON encode user_id
                elif operator == QueryOperator.IN:
                    placeholders = ','.join(['?' for _ in value])
                    where_conditions.append(f"json_extract(data, '$.{field}') IN ({placeholders})")
                    params.extend([json.dumps(v) if isinstance(v, str) else v for v in value])
                elif operator == QueryOperator.BETWEEN:
                    where_conditions.append(f"json_extract(data, '$.{field}') BETWEEN ? AND ?")
                    params.extend([json.dumps(v) if isinstance(v, str) else v for v in value])
                elif operator == QueryOperator.CONTAINS:
                    # Handle array contains operation
                    where_conditions.append(f"json_extract(data, '$.{field}') LIKE ?")
                    params.append(f'%{json.dumps(value)[1:-1]}%')
                elif operator == QueryOperator.EQ:
                    # Handle exact equality
                    if '.' in field:
                        # Handle nested fields
                        where_conditions.append(f"json_extract(data, '$.{field}') = ?")
                        params.append(json.dumps(value))  # Always JSON encode nested field values
                    else:
                        # Handle regular fields
                        where_conditions.append(f"json_extract(data, '$.{field}') = ?")
                        params.append(json.dumps(value) if isinstance(value, str) else value)
                else:
                    # Handle other operators
                    if '.' in field:
                        # Handle nested fields
                        where_conditions.append(f"json_extract(data, '$.{field}') {operator.value} ?")
                        params.append(json.dumps(value))  # Always JSON encode nested field values
                    else:
                        # Handle regular fields
                        where_conditions.append(f"json_extract(data, '$.{field}') {operator.value} ?")
                        params.append(json.dumps(value) if isinstance(value, str) else value)
            
            # Always add collection condition
            where_conditions.append("collection = ?")
            params.append(query.collection)
            
            if where_conditions:
                sql_parts.append("WHERE " + " AND ".join(where_conditions))
            
            if query.sort_fields:
                sort_clauses = [
                    f"json_extract(data, '$.{field}') {direction}"
                    for field, direction in query.sort_fields
                ]
                sql_parts.append("ORDER BY " + ", ".join(sort_clauses))
            
            if query.limit_value is not None:
                sql_parts.append("LIMIT ?")
                params.append(query.limit_value)
                
                if query.skip_value is not None:
                    sql_parts.append("OFFSET ?")
                    params.append(query.skip_value)
            
            sql = " ".join(sql_parts)
            cursor.execute(sql, params)
            return [{"_id": row[0], **json.loads(row[1])} for row in cursor]
    
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
                        current[parts[-1]] = value
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
    
    def close(self):
        """Close all database connections."""
        self.pool.close_all()