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
        try:
            self.pool = ConnectionPool(db_path, max_connections)
        except sqlite3.Error as e:
            raise RuntimeError(f"Failed to initialize database connection pool: {e}")
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
            ops = BulkOperations(conn)
            ops._connection_from_pool = True  # Mark as coming from pool
            return ops
    
    def _init_db(self) -> None:
        """Initialize database schema with proper transaction handling."""
        with self.pool.get_connection() as conn:
            try:
                # Set PRAGMA settings outside transaction
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL")
                
                # Create schema in transaction
                conn.execute("BEGIN")
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS collections (
                        name TEXT PRIMARY KEY,
                        metadata TEXT DEFAULT '{}',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS documents (
                        id TEXT PRIMARY KEY,
                        collection TEXT,
                        data TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (collection) REFERENCES collections(name)
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS indexes (
                        name TEXT PRIMARY KEY,
                        collection TEXT NOT NULL,
                        fields TEXT NOT NULL,
                        type TEXT NOT NULL DEFAULT 'btree',
                        unique_index INTEGER NOT NULL DEFAULT 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (collection) REFERENCES collections(name)
                    )
                """)
                # Create helpful indexes for better performance
                conn.execute("CREATE INDEX IF NOT EXISTS idx_collection ON documents(collection)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_collection_id ON documents(collection, id)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_indexes_collection ON indexes(collection)")
                conn.commit()
            except sqlite3.Error as e:
                conn.rollback()
                raise RuntimeError(f"Failed to initialize database schema: {e}")
    
    def create_index(self, collection: str, fields: Union[str, List[str]], 
                    index_type: str = "btree", unique: bool = False,
                    full_text: bool = False) -> str:
        """
        Create an index on specified fields.
        
        Args:
            collection: Collection name
            fields: Field or fields to index
            index_type: Type of index (btree, hash, etc.)
            unique: Whether the index should enforce uniqueness
            full_text: Whether to create a full-text search index
        """
        if isinstance(fields, str):
            fields = [fields]
        
        # Generate index name
        safe_fields = [field.replace('.', '_') for field in fields]
        index_prefix = "fts_" if full_text else "idx_"
        index_name = f"{index_prefix}{collection}_{'_'.join(safe_fields)}"
        
        with self.pool.get_connection() as conn:
            try:
                conn.execute("BEGIN")
                
                if full_text:
                    # For full-text search, we need to create a virtual table
                    # and keep it in sync with the documents table
                    
                    # Check if FTS5 module is available
                    try:
                        conn.execute("SELECT sqlite_source_id()")
                        sqlite_version = conn.execute("SELECT sqlite_version()").fetchone()[0]
                        has_fts5 = sqlite_version >= "3.9.0"
                    except:
                        has_fts5 = False
                    
                    fts_version = "fts5" if has_fts5 else "fts4"
                    
                    # Create the virtual table
                    field_definitions = ', '.join(f"{field.replace('.', '_')}" for field in fields)
                    conn.execute(f"""
                        CREATE VIRTUAL TABLE IF NOT EXISTS {index_name} 
                        USING {fts_version}(collection, id, {field_definitions}, tokenize=porter)
                    """)
                    
                    # Insert existing data
                    field_extracts = []
                    for field in fields:
                        extract = f"json_extract(data, '$.{field}')"
                        field_extracts.append(extract)
                    
                    conn.execute(f"""
                        INSERT INTO {index_name} (collection, id, {', '.join(safe_fields)})
                        SELECT collection, id, {', '.join(field_extracts)}
                        FROM documents
                        WHERE collection = ?
                    """, [collection])
                    
                    # Create triggers to keep the FTS index in sync
                    # Insert trigger
                    conn.execute(f"""
                        CREATE TRIGGER IF NOT EXISTS {index_name}_insert AFTER INSERT ON documents
                        WHEN new.collection = '{collection}'
                        BEGIN
                            INSERT INTO {index_name} (collection, id, {', '.join(safe_fields)})
                            VALUES (new.collection, new.id, {', '.join(f"json_extract(new.data, '$.{field}')" for field in fields)});
                        END
                    """)
                    
                    # Update trigger
                    conn.execute(f"""
                        CREATE TRIGGER IF NOT EXISTS {index_name}_update AFTER UPDATE ON documents
                        WHEN new.collection = '{collection}'
                        BEGIN
                            DELETE FROM {index_name} WHERE id = old.id;
                            INSERT INTO {index_name} (collection, id, {', '.join(safe_fields)})
                            VALUES (new.collection, new.id, {', '.join(f"json_extract(new.data, '$.{field}')" for field in fields)});
                        END
                    """)
                    
                    # Delete trigger
                    conn.execute(f"""
                        CREATE TRIGGER IF NOT EXISTS {index_name}_delete AFTER DELETE ON documents
                        WHEN old.collection = '{collection}'
                        BEGIN
                            DELETE FROM {index_name} WHERE id = old.id;
                        END
                    """)
                else:
                    # Traditional index
                    # Store index metadata
                    conn.execute("""
                        INSERT OR REPLACE INTO indexes 
                        (name, collection, fields, type, unique_index, created_at) 
                        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """, (index_name, collection, json.dumps(fields), index_type, int(unique)))
                    
                    # Create the actual SQLite index
                    field_exprs = []
                    for field in fields:
                        if '.' in field:
                            # Handle nested fields
                            expr = f"json_extract(data, '$.{field}')"
                        else:
                            expr = f"json_extract(data, '$.{field}')"
                        field_exprs.append(expr)
                    
                    # Create index with uniqueness constraint if specified
                    # Note: Collection name is hardcoded in WHERE clause since SQLite doesn't allow parameters there
                    index_sql = f"""
                        CREATE {'UNIQUE' if unique else ''} INDEX IF NOT EXISTS {index_name}
                        ON documents({', '.join(['collection'] + field_exprs)})
                        WHERE collection = '{collection}'
                    """
                    conn.execute(index_sql)
                
                conn.commit()
                return index_name
                
            except sqlite3.Error as e:
                conn.rollback()
                raise  # Re-raise the original error to preserve error type
    
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
        """
        Drop an index by name.
        
        Args:
            index_name: Name of the index to drop
        """
        with self.pool.get_connection() as conn:
            try:
                conn.execute("BEGIN")
                
                # Check if it's a FTS index
                if index_name.startswith("fts_"):
                    # Drop the FTS virtual table
                    conn.execute(f"DROP TABLE IF EXISTS {index_name}")
                    
                    # Drop the associated triggers
                    conn.execute(f"DROP TRIGGER IF EXISTS {index_name}_insert")
                    conn.execute(f"DROP TRIGGER IF EXISTS {index_name}_update")
                    conn.execute(f"DROP TRIGGER IF EXISTS {index_name}_delete")
                else:
                    # Drop regular index
                    conn.execute(f"DROP INDEX IF EXISTS {index_name}")
                
                # Remove from indexes table
                conn.execute("DELETE FROM indexes WHERE name = ?", [index_name])
                
                conn.commit()
            except sqlite3.Error as e:
                conn.rollback()
                raise RuntimeError(f"Failed to drop index {index_name}: {e}")
    
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
            elif op == QueryOperator.REGEX:
                conditions.append(f"json_extract(data, '$.{field}') REGEXP ?")
                params.append(value)
            elif op == QueryOperator.STARTS_WITH:
                conditions.append(f"json_extract(data, '$.{field}') LIKE ?")
                params.append(f"{value}%")
            elif op == QueryOperator.ENDS_WITH:
                conditions.append(f"json_extract(data, '$.{field}') LIKE ?")
                params.append(f"%{value}")
            elif op == QueryOperator.BETWEEN:
                conditions.append(f"json_extract(data, '$.{field}') BETWEEN ? AND ?")
                params.extend(value)  # value should be a list of [start, end]
            elif op == QueryOperator.IN:
                placeholders = ','.join(['?' for _ in value])
                conditions.append(f"json_extract(data, '$.{field}') IN ({placeholders})")
                params.extend(value)
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
    
    def backup(self, destination_path: str) -> bool:
        """
        Create a backup of the database.
        
        Args:
            destination_path: Path to save the backup to
            
        Returns:
            True if backup was successful, False otherwise
        """
        with self.pool.get_connection() as conn:
            try:
                # Use SQLite's built-in backup API
                backup_conn = sqlite3.connect(destination_path)
                conn.backup(backup_conn)
                backup_conn.close()
                return True
            except (sqlite3.Error, IOError) as e:
                raise RuntimeError(f"Failed to create backup: {e}")
    
    def restore(self, backup_path: str) -> bool:
        """
        Restore the database from a backup.
        
        Args:
            backup_path: Path to the backup file
            
        Returns:
            True if restore was successful, False otherwise
        """
        # Close all connections first
        self.close()
        
        try:
            # Open backup file
            backup_conn = sqlite3.connect(backup_path)
            # Open destination database
            dest_conn = sqlite3.connect(self.db_path)
            # Perform restore
            backup_conn.backup(dest_conn)
            # Clean up
            backup_conn.close()
            dest_conn.close()
            
            # Reinitialize connection pool
            self.pool = ConnectionPool(self.db_path, self.pool.max_connections)
            return True
        except (sqlite3.Error, IOError) as e:
            # Attempt to reinitialize connection pool in case of failure
            try:
                self.pool = ConnectionPool(self.db_path, self.pool.max_connections)
            except:
                pass
            raise RuntimeError(f"Failed to restore from backup: {e}")
    
    def close(self):
        """Close all database connections."""
        # Close connections in the same thread they were created
        with self.pool.get_connection() as conn:
            conn.close()
        self.pool.close_all()