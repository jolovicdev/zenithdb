import sqlite3
from typing import Dict, Generator
import threading
from contextlib import contextmanager
import time

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
        self.connection_timeout = 30  # seconds
        self.max_connection_age = 3600  # 1 hour
        self._connections = {}
        self._active_connections = set()  # Track active connection IDs
        self._lock = threading.Lock()
        self._connection_timestamps = {}
    
    @contextmanager
    def get_connection(self) -> Generator[sqlite3.Connection, None, None]:
        """
        Get a connection from the pool.
        
        Returns:
            A SQLite connection object
        
        Raises:
            Exception: If the connection pool is exhausted
        """
        thread_id = threading.get_ident()
        conn_id = len(self._active_connections)  # Use connection count as ID
        
        with self._lock:
            if len(self._active_connections) >= self.max_connections:
                raise Exception("Connection pool exhausted")
            
            if thread_id not in self._connections:
                conn = sqlite3.connect(self.db_path)
                conn.row_factory = sqlite3.Row
                self._connections[thread_id] = conn
                self._connection_timestamps[thread_id] = time.time()
            
            self._active_connections.add(conn_id)
        
        try:
            yield self._connections[thread_id]
        finally:
            with self._lock:
                if conn_id in self._active_connections:
                    self._active_connections.remove(conn_id)
                
                current_time = time.time()
                if current_time - self._connection_timestamps[thread_id] > self.max_connection_age:
                    self._connections[thread_id].close()
                    conn = sqlite3.connect(self.db_path)
                    conn.row_factory = sqlite3.Row
                    self._connections[thread_id] = conn
                    self._connection_timestamps[thread_id] = current_time
    
    def close_all(self):
        """Close all connections in the pool."""
        with self._lock:
            for thread_id, conn in self._connections.items():
                try:
                    conn.close()
                except sqlite3.Error:
                    pass
            self._connections.clear()
            self._connection_timestamps.clear()
            self._active_connections.clear()
    
    def _check_connection_health(self, conn: sqlite3.Connection) -> bool:
        """Check if connection is healthy."""
        try:
            conn.execute("SELECT 1").fetchone()
            return True
        except sqlite3.Error:
            return False
    
    def _cleanup_dead_connections(self):
        """Clean up any dead connections from threads that no longer exist."""
        current_threads = set(threading.enumerate())
        current_thread_ids = {thread.ident for thread in current_threads if thread.ident is not None}
        
        to_remove = []
        for thread_id in list(self._connections.keys()):
            if thread_id not in current_thread_ids:
                to_remove.append(thread_id)
        
        for thread_id in to_remove:
            try:
                self._connections[thread_id].close()
            except sqlite3.Error:
                pass
            del self._connections[thread_id]
            if thread_id in self._connection_timestamps:
                del self._connection_timestamps[thread_id]