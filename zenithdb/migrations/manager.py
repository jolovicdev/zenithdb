import sqlite3
from typing import List, Callable
from ..core.connection_pool import ConnectionPool

class Migration:
    """Represents a single database migration."""
    
    def __init__(self, version: str, up: Callable[[sqlite3.Connection], None], 
                 down: Callable[[sqlite3.Connection], None]):
        """
        Initialize a new migration.
        
        Args:
            version: Migration version string
            up: Function to apply the migration
            down: Function to revert the migration
        """
        self.version = version
        self.up = up
        self.down = down

class MigrationManager:
    """Manages database migrations."""
    
    def __init__(self, connection_pool: ConnectionPool):
        """
        Initialize the migration manager.
        
        Args:
            connection_pool: Database connection pool
        """
        self.pool = connection_pool
        self.migrations: List[Migration] = []
        self._init_migrations_table()
    
    def _init_migrations_table(self):
        """Initialize the migrations tracking table."""
        with self.pool.get_connection() as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS migrations (
                    version TEXT PRIMARY KEY,
                    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            conn.commit()
    
    def register_migration(self, migration: Migration):
        """
        Register a new migration.
        
        Args:
            migration: Migration to register
        """
        self.migrations.append(migration)
        self.migrations.sort(key=lambda x: x.version)
    
    def get_current_version(self) -> str:
        """
        Get the current database version.
        
        Returns:
            Current version string or '0' if no migrations applied
        """
        with self.pool.get_connection() as conn:
            cursor = conn.execute(
                'SELECT version FROM migrations ORDER BY version DESC LIMIT 1'
            )
            result = cursor.fetchone()
            return result[0] if result else '0'
    
    def migrate_up(self, target_version: str = None):
        """
        Apply migrations up to target_version.
        
        Args:
            target_version: Target version to migrate to (optional)
        """
        current = self.get_current_version()
        
        for migration in self.migrations:
            if migration.version <= current:
                continue
            if target_version and migration.version > target_version:
                break
                
            with self.pool.get_connection() as conn:
                try:
                    migration.up(conn)
                    conn.execute(
                        'INSERT INTO migrations (version) VALUES (?)',
                        (migration.version,)
                    )
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    raise e
    
    def migrate_down(self, target_version: str):
        """
        Revert migrations down to target_version.
        
        Args:
            target_version: Target version to migrate to
        """
        current = self.get_current_version()
        
        for migration in reversed(self.migrations):
            if migration.version > current or migration.version <= target_version:
                continue
                
            with self.pool.get_connection() as conn:
                try:
                    migration.down(conn)
                    conn.execute(
                        'DELETE FROM migrations WHERE version = ?',
                        (migration.version,)
                    )
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    raise e 