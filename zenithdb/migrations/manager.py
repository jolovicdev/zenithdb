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
    
    def __init__(self, db_or_pool):
        """
        Initialize the migration manager.
        
        Args:
            db_or_pool: Database or ConnectionPool instance
        """
        self.pool = db_or_pool.pool if hasattr(db_or_pool, 'pool') else db_or_pool
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
    
    def init_migrations_table(self):
        """Public method to initialize migrations table."""
        with self.pool.get_connection() as conn:
            conn.execute('DELETE FROM migrations')  # Clear existing migrations
            conn.commit()
        self._init_migrations_table()
    
    def apply_migration(self, migration_data: dict):
        """
        Apply a single migration from dictionary data.
        
        Args:
            migration_data: Dictionary containing migration data
            
        Raises:
            ValueError: If migration data is invalid
        """
        if not isinstance(migration_data, dict):
            raise ValueError("Migration data must be a dictionary")
            
        required_fields = ['version', 'name', 'up', 'down']
        if not all(field in migration_data for field in required_fields):
            raise ValueError(f"Migration must contain fields: {', '.join(required_fields)}")
            
        # Check for duplicate version
        if any(m.version == migration_data['version'] for m in self.migrations):
            raise ValueError(f"Migration version {migration_data['version']} already exists")
            
        # Wrap the functions to handle connection
        def wrap_func(func):
            if func.__code__.co_argcount == 0:
                return lambda conn: func()
            return func
            
        migration = Migration(
            version=migration_data['version'],
            up=wrap_func(migration_data['up']),
            down=wrap_func(migration_data['down'])
        )
        self.register_migration(migration)
        self.migrate_up(migration.version)
    
    def get_applied_migrations(self) -> list:
        """
        Get list of applied migrations.
        
        Returns:
            List of applied migration versions
        """
        with self.pool.get_connection() as conn:
            cursor = conn.execute('SELECT version, applied_at FROM migrations ORDER BY version')
            return [dict(row) for row in cursor.fetchall()]
    
    def rollback_migration(self, migration_data: dict):
        """
        Rollback a specific migration.
        
        Args:
            migration_data: Migration data to rollback
        """
        if not isinstance(migration_data, dict) or 'version' not in migration_data:
            raise ValueError("Invalid migration data")
            
        with self.pool.get_connection() as conn:
            # Find the migration to rollback
            cursor = conn.execute('SELECT version FROM migrations WHERE version = ?', 
                                (migration_data['version'],))
            if not cursor.fetchone():
                return  # Migration not applied
                
            # Execute down migration
            migration = next((m for m in self.migrations if m.version == migration_data['version']), None)
            if migration:
                try:
                    migration.down(conn)
                    conn.execute('DELETE FROM migrations WHERE version = ?', 
                               (migration_data['version'],))
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    raise e 
    
    def apply_migration(self, migration_data: dict):
        """
        Apply a single migration from dictionary data.
        
        Args:
            migration_data: Dictionary containing migration data
            
        Raises:
            ValueError: If migration data is invalid
        """
        if not isinstance(migration_data, dict):
            raise ValueError("Migration data must be a dictionary")
            
        required_fields = ['version', 'name', 'up', 'down']
        if not all(field in migration_data for field in required_fields):
            raise ValueError(f"Migration must contain fields: {', '.join(required_fields)}")
            
        # Check for duplicate version
        if any(m.version == migration_data['version'] for m in self.migrations):
            raise ValueError(f"Migration version {migration_data['version']} already exists")
            
        # Wrap the functions to handle connection
        def wrap_func(func):
            if func.__code__.co_argcount == 0:
                return lambda conn: func()
            return func
            
        migration = Migration(
            version=migration_data['version'],
            up=wrap_func(migration_data['up']),
            down=wrap_func(migration_data['down'])
        )
        self.register_migration(migration)
        self.migrate_up(migration.version) 