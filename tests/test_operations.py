import pytest
from zenithdb.operations import BulkOperations
import sqlite3

@pytest.fixture
def db_connection():
    """Create a test database connection."""
    conn = sqlite3.connect(':memory:')
    conn.execute('''CREATE TABLE documents
                   (id TEXT PRIMARY KEY, collection TEXT, data TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    yield conn
    conn.close()

def test_bulk_operations_init(db_connection):
    """Test bulk operations initialization."""
    ops = BulkOperations(db_connection)
    assert ops.retry_count == 3
    assert ops.progress_callback is None
    
    with pytest.raises(TypeError):
        BulkOperations("not a connection")

def test_bulk_operations_transaction(db_connection):
    """Test transaction handling."""
    ops = BulkOperations(db_connection)
    
    # Test successful transaction
    with ops:
        ops.bulk_insert("test", [{"key": "value"}])
        assert db_connection.in_transaction
    assert not db_connection.in_transaction
    
    # Test failed transaction
    with pytest.raises(Exception):
        with ops:
            ops.bulk_insert("test", [{"key": "value"}])
            raise Exception("Test error")
    assert not db_connection.in_transaction

def test_bulk_operations_progress(db_connection):
    """Test progress callback."""
    ops = BulkOperations(db_connection)
    progress_calls = []
    
    def progress_callback(current, total):
        progress_calls.append((current, total))
    
    ops.set_progress_callback(progress_callback)
    
    docs = [{"key": f"value{i}"} for i in range(100)]
    ops.bulk_insert("test", docs)
    
    assert len(progress_calls) > 0
    assert progress_calls[-1][1] == 100

def test_batch_size_optimization(db_connection):
    """Test batch size optimization."""
    ops = BulkOperations(db_connection)
    
    # Test with small documents
    small_docs = [{"key": "small"} for _ in range(1000)]
    batch_size = ops._calculate_optimal_batch_size(small_docs)
    assert batch_size == 1000
    
    # Test with large documents
    large_docs = [{"key": "x" * 10000} for _ in range(1000)]
    batch_size = ops._calculate_optimal_batch_size(large_docs)
    assert batch_size < 1000