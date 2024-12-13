# ZenithDB

SQLite-powered document database with MongoDB-like syntax, full-text search, and advanced querying capabilities.
For complete examples of all features, PLEASE check out [usage.py](usage.py).

## Features

- **Document Storage & Validation**: Store and validate JSON-like documents with nested structures
- **Advanced Querying**: Full-text search, nested field queries, array operations
- **Multiple Query Styles**: Support for both MongoDB-style dict queries and fluent Query builder
- **Indexing**: Single and compound indexes for optimized performance
- **Aggregations**: Group and aggregate data with functions like COUNT, AVG, SUM
- **Bulk Operations**: Efficient batch processing with transaction support
- **Connection Pooling**: Built-in connection pool for concurrent operations
- **Migration Support**: Versioned database migrations with up/down functions

## Installation

```bash
pip install zenithdb
```

## Quick Start

```python
from zenithdb import Database

# Initialize database
db = Database("myapp.db")
users = db.collection("users")

# Add document validation
def age_validator(doc):
    return isinstance(doc.get('age'), int) and doc['age'] >= 0
users.set_validator(age_validator)

# Insert documents
users.insert({
    "name": "John Doe",
    "age": 30,
    "tags": ["premium"],
    "profile": {"city": "New York"}
})

# Query documents
users.find({
    "age": {"$gt": 25},
    "tags": {"$contains": "premium"}
})

# Full-text search
users.find({"*": {"$contains": "John"}})

# Nested updates
users.update(
    {"name": "John Doe"},
    {"$set": {
        "profile.city": "Brooklyn",
        "tags.0": "vip"
    }}
)

# Aggregations
users.aggregate([{
    "group": {
        "field": "profile.city",
        "function": "COUNT",
        "alias": "count"
    }
}])
```

## Collection Management

```python
# List and count collections
db.list_collections()
db.count_collections()

# Drop collections
db.drop_collection("users")
db.drop_all_collections()

# Print collection contents
users.print_collection()
users.count()
```

## Advanced Features

### Indexing
```python
# Create indexes
db.create_index("users", ["email"])
db.create_index("users", ["profile.city", "age"])

# List indexes
db.list_indexes("users")
```

### Bulk Operations
```python
bulk_ops = users.bulk_operations()
with bulk_ops.transaction():
    bulk_ops.bulk_insert("users", [
        {"name": "User1", "age": 31},
        {"name": "User2", "age": 32}
    ])
```

### Migrations
```python
from zenithdb.migrations import MigrationManager

manager = MigrationManager(db)
migration = {
    'version': '001',
    'name': 'add_users',
    'up': lambda: db.collection('users').insert({'admin': True}),
    'down': lambda: db.collection('users').delete({})
}
manager.apply_migration(migration)
```

## Development

```bash
# Install development dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/
pytest tests/test_migrations.py
pytest --cov=zenithdb tests/
```

For complete examples of all features, check out [usage.py](usage.py).
I would not recommend using this as a production database, but it's a fun project to play around with.