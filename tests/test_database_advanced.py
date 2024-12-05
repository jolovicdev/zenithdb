import pytest
import os
import sqlite3
import threading
from zenithdb import Database

@pytest.fixture
def db():
    """Create a test database."""
    db_path = "test_advanced.db"
    if os.path.exists(db_path):
        os.remove(db_path)
    db = Database(db_path)
    yield db
    db.close()
    if os.path.exists(db_path):
        os.remove(db_path)

def test_connection_pool_limits(db):
    """Test connection pool limits."""
    # Create connections up to the limit
    connections = []
    connection_contexts = []  # Keep context managers alive
    
    try:
        for i in range(db.pool.max_connections):
            ctx = db.pool.get_connection()
            conn = ctx.__enter__()
            connection_contexts.append(ctx)
            connections.append(conn)
            # Use database's health check method
            assert db._check_connection_health(conn)
        
        # Try to get one more connection - should raise
        with pytest.raises(Exception, match="Connection pool exhausted"):
            with db.pool.get_connection() as conn:
                pass
    finally:
        # Clean up connections
        for ctx in connection_contexts:
            ctx.__exit__(None, None, None)

def test_index_operations(db):
    """Test advanced index operations."""
    # Test creating complex index
    idx_name = db.create_index("users", ["profile.age", "tags"])
    assert idx_name in [idx["name"] for idx in db.list_indexes("users")]
    
    # Test creating duplicate index (should not raise)
    db.create_index("users", ["profile.age", "tags"])
    
    # Test invalid index
    with pytest.raises(sqlite3.OperationalError):
        db.create_index("users", "invalid[]")

def test_concurrent_operations(db):
    """Test concurrent database operations."""
    results = []
    def worker():
        try:
            with db.pool.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                results.append(True)
        except Exception:
            results.append(False)
    
    threads = [threading.Thread(target=worker) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    
    assert all(results)