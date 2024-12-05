import sqlite3
from typing import Dict
import threading
from contextlib import contextmanager

class ConnectionPool:
    """A thread-safe connection pool for SQLite connections."""
    
    def __init__(self, db_path: str, max_connections: int = 10):
        """
        Initialize a new connection pool.
        
        Args:
            db_path: Path to the SQLite database file
            max_connections: Maximum number of concurrent connections
        """
        self.db_path = db_path
        self.max_connections = max_connections
        self._connections: Dict[int, sqlite3.Connection] = {}
        self._lock = threading.Lock()
        
    @contextmanager
    def get_connection(self) -> sqlite3.Connection:
        """
        Get a connection from the pool.
        
        Returns:
            A SQLite connection object
        
        Raises:
            Exception: If the connection pool is exhausted
        """
        thread_id = threading.get_ident()
        
        with self._lock:
            if thread_id not in self._connections:
                if len(self._connections) >= self.max_connections:
                    raise Exception("Connection pool exhausted")
                
                conn = sqlite3.connect(self.db_path)
                conn.row_factory = sqlite3.Row
                self._connections[thread_id] = conn
            
        try:
            yield self._connections[thread_id]
        finally:
            pass
    
    def close_all(self):
        """Close all connections in the pool."""
        with self._lock:
            for conn in self._connections.values():
                conn.close()
            self._connections.clear() 