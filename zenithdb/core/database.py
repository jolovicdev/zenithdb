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
    
    def __init__(self, db_path: str, max_connections: int = 10, max_result_size: int = 10000, debug: bool = False):
        """Initialize database with connection pool."""
        self.db_path = db_path
        self.pool = ConnectionPool(db_path, max_connections)
        self.max_result_size = max_result_size
        self.debug = debug
        self._init_db()
        self._collections: Dict[str, Collection] = {}
    
    def collection(self, name: str) -> Collection:
        """
        Get or create a collection interface.
        Returns cached collection instance if it exists, otherwise creates new one.
        """
        if name not in self._collections:
            self._collections[name] = Collection(self, name)
        return self._collections[name]
    
    def list_collections(self) -> List[str]:
        """Get list of all collection names in the database."""
        with self.pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT name FROM collections ORDER BY name")
            return sorted([row[0] for row in cursor.fetchall()])

    def count_collections(self) -> int:
        """Get count of all collections in the database."""
        with self.pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM collections")
            return cursor.fetchone()[0]

    def drop_all_collections(self) -> None:
        """Drop all collections in the database."""
        with self.pool.get_connection() as conn:
            conn.execute("DELETE FROM collections")
            conn.execute("DELETE FROM documents")
            conn.execute("DELETE FROM indexes")
            conn.commit()
    
    def drop_collection(self, name: str) -> None:
        """
        Drop a collection and all its documents.
        Also removes any associated indexes.
        """
        with self.pool.get_connection() as conn:
            # Delete collection metadata
            conn.execute("DELETE FROM collections WHERE name = ?", [name])
            
            # Delete all documents in collection
            conn.execute("DELETE FROM documents WHERE collection = ?", [name])
            
            # Delete associated indexes
            indexes = self.list_indexes(name)
            for index in indexes:
                self.drop_index(index['name'])
            
            # Remove from cache
            if name in self._collections:
                try:
                    del self._collections[name]
                except KeyError:
                    # Collection not found in cache, ignore
                    pass
            
            conn.commit()

    def print_everything(self) -> List[Dict]:
        """Print and return all database contents in a readable format."""
        results = {
            'collections': [],
            'documents': [],
            'indexes': []
        }
        
        with self.pool.get_connection() as conn:
            cursor = conn.cursor()
            
            # Get collections
            cursor.execute("SELECT name, created_at, updated_at, metadata FROM collections")
            collections = [
                {
                    'name': row[0],
                    'created_at': row[1],
                    'updated_at': row[2],
                    'metadata': json.loads(row[3]) if row[3] else None
                }
                for row in cursor.fetchall()
            ]
            results['collections'] = collections
            print("\nCollections:")
            for col in collections:
                print(f"  - {col['name']} (created: {col['created_at']})")
            
            # Get documents
            cursor.execute("SELECT collection, id, data, created_at, updated_at FROM documents")
            documents = [
                {
                    'collection': row[0],
                    'id': row[1],
                    'data': json.loads(row[2]),
                    'created_at': row[3],
                    'updated_at': row[4]
                }
                for row in cursor.fetchall()
            ]
            results['documents'] = documents
            print("\nDocuments:")
            for doc in documents:
                print(f"  - [{doc['collection']}] {doc['id']}: {json.dumps(doc['data'], indent=2)[:100]}...")
            
            # Get indexes
            cursor.execute("SELECT name, collection, fields, type, unique_index FROM indexes")
            indexes = [
                {
                    'name': row[0],
                    'collection': row[1],
                    'fields': json.loads(row[2]),
                    'type': row[3],
                    'unique': bool(row[4])
                }
                for row in cursor.fetchall()
            ]
            results['indexes'] = indexes
            print("\nIndexes:")
            for idx in indexes:
                unique_str = " (unique)" if idx['unique'] else ""
                print(f"  - {idx['name']}: {idx['collection']}.{idx['fields']}{unique_str}")
            
            conn.commit()
            
        return results

    def bulk_operations(self) -> BulkOperations:
        """Get bulk operations interface."""
        with self.pool.get_connection() as conn:
            return BulkOperations(conn)
    
    def _init_db(self):
        """Initialize database with optimized settings."""
        with self.pool.get_connection() as conn:
            conn.executescript('''
                PRAGMA journal_mode=WAL;          -- WAL mode for concurrent reads and writes
                PRAGMA synchronous=NORMAL;          -- Improves write performance
                PRAGMA temp_store=MEMORY;        -- Keeps temp tables in memory
                PRAGMA page_size=16384;          -- Larger pages reduce I/O
                PRAGMA cache_size=-20000;        -- Approx 80MB cache for limited memory systems
                PRAGMA auto_vacuum=NONE;         -- Disable auto vacuum to improve performance
                PRAGMA locking_mode=NORMAL;      -- Avoid exclusive locks for concurrency
                PRAGMA busy_timeout=10000;       -- 10s timeout for lock contention
                PRAGMA mmap_size=268435456;      -- 256MB memory-mapped file access
                PRAGMA journal_size_limit=67108864; -- Limit journal to 64MB
                PRAGMA threads=4;                -- Utilize 4 threads for processing
            ''')
            conn.executescript('''
                BEGIN IMMEDIATE;
                CREATE TABLE IF NOT EXISTS collections (
                    name TEXT PRIMARY KEY,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    metadata TEXT
                );
                CREATE TABLE IF NOT EXISTS documents (
                    id TEXT PRIMARY KEY,
                    collection TEXT NOT NULL,
                    data TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (collection) REFERENCES collections(name) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_collection ON documents(collection) WHERE collection IS NOT NULL;
                CREATE INDEX IF NOT EXISTS idx_collection_id ON documents(collection, id) WHERE collection IS NOT NULL;
                CREATE TABLE IF NOT EXISTS indexes (
                    name TEXT PRIMARY KEY,
                    collection TEXT NOT NULL,
                    fields TEXT NOT NULL,
                    type TEXT NOT NULL,
                    unique_index INTEGER NOT NULL,
                    FOREIGN KEY (collection) REFERENCES collections(name) ON DELETE CASCADE
                );
                COMMIT;
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
            conn.executescript(f"""
                {index_sql};
                ANALYZE;
                ANALYZE {index_name};
            """)
        
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
            conn.execute("BEGIN IMMEDIATE")
            try:
                ops = BulkOperations(conn)
                doc_id = ops.bulk_insert(collection, [document], [doc_id] if doc_id else None)[0]
                conn.commit()
                return doc_id
            except Exception as e:
                conn.rollback()
                raise e
    
    def check_index_usage(self, sql: str, params: List[Any] = None) -> bool:
        """Check if a query is using indexes and print the execution plan."""
        with self.pool.get_connection() as conn:
            cursor = conn.cursor()
            
            # Get the execution plan
            cursor.execute(f"EXPLAIN QUERY PLAN {sql}", params or [])
            plan = cursor.fetchall()
            
            # Parse the query plan
            using_index = False
            current_index = None
            
            for row in plan:
                detail = row[3]
                if "USING INDEX" in detail:
                    # Extract index name and conditions
                    parts = detail.split("USING INDEX")
                    index_name = parts[1].split()[0]
                    conditions = parts[1].split("(")[1].split(")")[0]
                    
                    # Check if it's using more than just the collection index
                    if len(conditions.split("AND")) > 1:
                        using_index = True
                        current_index = index_name
            
            if self.debug:  # Only print if debug mode is enabled
                if using_index:
                    print(f"✓ Using index: {current_index}")
                else:
                    print("✗ No index used - table scan")
            
            return using_index
    
    def execute_query(self, query: 'Query') -> List[Dict[str, Any]]:
        """Execute a query and return results with optimized execution."""
        conditions = []
        params = []
        
        # Add collection condition first for better index usage
        conditions.append("collection = ?")
        params.append(query.collection)
        
        for field, op, value in query.conditions:
            if value is None:
                conditions.append("""(
                    json_extract(data, ?) IS NULL 
                    AND json_type(data, ?) IS NOT NULL
                )""")
                params.extend([f"$.{field}", f"$.{field}"])
                continue
                
            if op == QueryOperator.CONTAINS:
                conditions.append(f"json_extract(data, '$.{field}') LIKE ?")
                params.append(f"%{value}%")
            elif op == QueryOperator.EQ:
                if field == "_id":
                    conditions.append("id = ?")
                    params.append(value)
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
        
        where_clause = " AND ".join(conditions)
        limit_clause = f"LIMIT {query.limit_value}" if query.limit_value else "LIMIT 10000"
        offset_clause = f"OFFSET {query.skip_value}" if query.skip_value else "OFFSET 0"
        
        sql = f"""
            SELECT data
            FROM documents 
            WHERE {where_clause}
            {limit_clause} {offset_clause}
        """
        
        # Check and print index usage
        self.check_index_usage(sql, params)
        
        with self.pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA read_uncommitted = ON")
            try:
                cursor.execute(sql, params)
            except sqlite3.OperationalError as e:
                if "no such index" not in str(e):
                    raise
                cursor.execute(sql, params)
            
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
        """Get the best index hint for the query based on available indexes."""
        if not conditions:
            return ""
            
        # Get all available indexes for collection
        indexes = self.list_indexes(collection)
        if not indexes:
            return ""
        
        # Extract fields from conditions
        query_fields = [(field, op) for field, op, _ in conditions]
        
        # First try to find a perfect match for compound indexes
        for index in indexes:
            index_fields = json.loads(index['fields']) if isinstance(index['fields'], str) else index['fields']
            if not isinstance(index_fields, list):
                index_fields = [index_fields]
            
            # Check if index fields match query fields prefix
            if any(field == index_fields[0] for field, _ in query_fields):
                return f"INDEXED BY {index['name']}"
        
        # If no perfect match, try to find an index that can help
        for index in indexes:
            index_fields = json.loads(index['fields']) if isinstance(index['fields'], str) else index['fields']
            if not isinstance(index_fields, list):
                index_fields = [index_fields]
            
            # Check if any query field uses this index
            for query_field, op in query_fields:
                if query_field in index_fields:
                    # For range queries, only use if it's the first field
                    if op in [QueryOperator.GT, QueryOperator.GTE, QueryOperator.LT, QueryOperator.LTE]:
                        if query_field == index_fields[0]:
                            return f"INDEXED BY {index['name']}"
                    else:
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