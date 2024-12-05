import os
import pytest
from zenithdb.migrations.manager import MigrationManager
from zenithdb import Database

@pytest.fixture
def db():
    """Create a test database."""
    db_path = "test_migrations.db"
    if os.path.exists(db_path):
        os.remove(db_path)
    db = Database(db_path)
    yield db
    db.close()
    if os.path.exists(db_path):
        os.remove(db_path)

def test_migration_manager(db):
    """Test migration manager functionality."""
    manager = MigrationManager(db)
    
    # Test creating migration table
    manager.init_migrations_table()
    assert manager.get_applied_migrations() == []
    
    # Test applying migration
    migration = {
        'version': '001',
        'name': 'test_migration',
        'up': lambda: db.collection('test').insert({'key': 'value'}),
        'down': lambda: db.collection('test').delete({})
    }
    
    manager.apply_migration(migration)
    applied = manager.get_applied_migrations()
    assert len(applied) == 1
    assert applied[0]['version'] == '001'
    
    # Test rollback
    manager.rollback_migration(migration)
    assert len(manager.get_applied_migrations()) == 0

def test_migration_errors(db):
    """Test migration error handling."""
    manager = MigrationManager(db)
    
    # Test invalid migration
    with pytest.raises(ValueError):
        manager.apply_migration({})
    
    # Test duplicate migration
    migration = {
        'version': '001',
        'name': 'test',
        'up': lambda: None,
        'down': lambda: None
    }
    
    manager.apply_migration(migration)
    with pytest.raises(ValueError):
        manager.apply_migration(migration)