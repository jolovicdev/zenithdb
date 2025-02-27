# ZenithDB 

![Version](https://img.shields.io/badge/version-2.0.0-blue.svg)
![Python](https://img.shields.io/badge/python-3.7%2B-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)

A high-performance document database built on SQLite with MongoDB-like syntax. ZenithDB combines the simplicity and reliability of SQLite with the flexibility of document databases.

## What is ZenithDB?

ZenithDB is a powerful, lightweight document database that provides a MongoDB-like experience while leveraging SQLite as the storage engine. It's perfect for:

- Applications that need document database features without the complexity of a separate server
- Projects requiring embedded databases with rich querying capabilities
- Developers who enjoy MongoDB's API but need SQLite's portability and simplicity
- Situations where a full-scale NoSQL database would be overkill

## 🚀 Key Features

- **Document Storage & Validation**: Store and validate JSON-like documents with nested structures
- **Advanced Querying**: Full-text search, nested field queries, array operations
- **Multiple Query Styles**: Support for both MongoDB-style dict queries and fluent Query builder
- **Indexing**: Single, compound, and full-text search indexes for optimized performance
- **Pattern Matching**: Regex, starts/ends with, and advanced text search capabilities
- **Advanced Aggregations**: Group and aggregate with COUNT, AVG, SUM, MEDIAN, STDDEV, and more
- **Bulk Operations**: Efficient batch processing with transaction support
- **Connection Pooling**: Built-in connection pool for concurrent operations
- **Migration Support**: Versioned database migrations with up/down functions
- **Backup & Restore**: Simple database backup and restore operations
- **Type Annotations**: Full Python type hints for better IDE integration
- **Performance Optimizations**: Query plan analysis, index utilization, and connection pooling

## 📦 Installation

```bash
pip install zenithdb
```

## 🚀 Quick Start

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

## 📚 Detailed Documentation

### Collection Management

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

### Advanced Querying

ZenithDB supports two querying styles:

#### MongoDB Style (Dict-based)

```python
# Find documents with comparison operators
users.find({
    "age": {"$gt": 25, "$lte": 50},
    "profile.city": "New York", 
    "tags": {"$contains": "premium"}
})

# Full-text search across all fields
results = users.find({"*": {"$contains": "search term"}})

# Pattern matching with regex, starts with, and ends with
users.find({
    "email": {"$regex": "^[a-z0-9]+@example\\.com$"},
    "name": {"$startsWith": "Jo"},
    "domain": {"$endsWith": ".org"}
})
```

#### Query Builder (Fluent API)

```python
from zenithdb import Query

q = Query()
results = users.find(
    (q.age > 25) & 
    (q.age <= 50) &
    (q.profile.city == "New York") &
    q.tags.contains("premium")
)

# Pattern matching with Query builder
results = users.find(
    q.email.regex("^[a-z0-9]+@example\\.com$") &
    q.name.starts_with("Jo") &
    q.domain.ends_with(".org")
)

# With sorting and pagination
q = Query(collection="users", database=db)
q.sort("age", ascending=False)
q.limit(10).skip(20)  # Page 3 with 10 items per page
results = q.execute()
```

### Indexing

```python
# Create single field index
db.create_index("users", "email")

# Create compound index
db.create_index("users", ["profile.city", "age"])

# Create unique index
db.create_index("users", "username", unique=True)

# Create full-text search index for efficient text searching
db.create_index("users", ["name", "bio", "tags"], full_text=True)

# Perform efficient text search using the FTS index
users.search_text("python developer")

# Search specific fields only
users.search_text("developer", fields=["bio"])

# List indexes and drop index
db.list_indexes("users")
db.drop_index("idx_users_email")
db.drop_index("fts_users_name_bio_tags")  # Drop a full-text search index
```

### Bulk Operations and Transactions

```python
# Get bulk operations interface
bulk_ops = users.bulk_operations()

# Use transaction for atomic operations
with bulk_ops.transaction():
    # Bulk insert
    ids = bulk_ops.bulk_insert("users", [
        {"name": "User1", "age": 31},
        {"name": "User2", "age": 32}
    ])
    
    # Bulk update
    bulk_ops.bulk_update("users", [
        {"_id": ids[0], "status": "active"},
        {"_id": ids[1], "status": "inactive"}
    ])
```

### Migrations

```python
from zenithdb.migrations import MigrationManager

# Initialize migration manager
manager = MigrationManager(db)

# Create migration
migration = {
    'version': '001',
    'name': 'add_users',
    'up': lambda: db.collection('users').insert({'admin': True}),
    'down': lambda: db.collection('users').delete({})
}

# Apply migration
manager.apply_migration(migration)

# Get current version
current_version = manager.get_current_version()
```

### Backup & Restore

```python
# Backup the database to a file
db.backup("backup_2025_02_27.db")

# Restore from a backup file
db.restore("backup_2025_02_27.db")
```

### Advanced Aggregations

ZenithDB 2.0 includes advanced aggregation functions beyond basic COUNT, SUM, and AVG:

```python
from zenithdb import AggregateFunction

# Calculate median age by country
median_age = users.aggregate([{
    "group": {
        "field": "profile.location.country",
        "function": AggregateFunction.MEDIAN,
        "target": "age",
        "alias": "median_age"
    }
}])

# Calculate standard deviation of salaries
salary_stddev = users.aggregate([{
    "group": {
        "field": "department",
        "function": AggregateFunction.STDDEV,
        "target": "salary",
        "alias": "salary_stddev"
    }
}])

# Count distinct values
distinct_cities = users.aggregate([{
    "group": {
        "field": "profile.location.country",
        "function": AggregateFunction.COUNT_DISTINCT,
        "target": "profile.location.city",
        "alias": "unique_cities"
    }
}])
```

## 🔍 Performance Optimization

ZenithDB includes tools for monitoring and optimizing query performance:

```python
# Initialize with debugging for query plan analysis
db = Database("myapp.db", debug=True)

# Create appropriate indexes
db.create_index("users", ["age", "status"])

# Run query to see if indexes are used
users.find({"age": {"$gt": 25}, "status": "active"})
# Output: ✓ Using index: idx_users_age_status
```

## 🧰 Development

```bash
# Install development dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/
pytest tests/test_migrations.py
pytest --cov=zenithdb tests/
```

## 🔄 Migrating from v1.x to v2.0

Version 2.0 of ZenithDB introduces several improvements with minimal breaking changes:

### New Features

- **Advanced Querying**: New regex, starts_with, and ends_with pattern matching
- **Full-Text Search**: Optimized full-text search with dedicated indexes
- **Advanced Aggregations**: MEDIAN, STDDEV, and COUNT_DISTINCT functions
- **Backup & Restore**: Database backup and restore capabilities
- **Extended Database Tools**: More utilities for managing SQLite databases

### Improvements

- **Connection Management**: Improved connection pooling and cleanup
- **Transaction Handling**: Enhanced transaction support with better error recovery
- **Query Performance**: Optimized indexing and query planning
- **Bug Fixes**: Fixed issues with nested document updates and array operations

### Migration Steps

To migrate from v1.x:

1. Update your installation: `pip install --upgrade zenithdb`
2. Review and update any custom bulk operations to use the transaction context
3. No database schema changes required - existing databases will work with the new version
4. Consider using new features like full-text indexes to improve search performance

## 📋 Comparison with Other Solutions

| Feature | ZenithDB | SQLite | MongoDB | TinyDB |
|---------|---------|--------|---------|--------|
| Document Storage | ✅ | ❌ (requires JSON) | ✅ | ✅ |
| Nested Queries | ✅ | ❌ (limited) | ✅ | ✅ |
| Full-text Search | ✅ | ❌ (needs extension) | ✅ | ❌ |
| Indexing | ✅ | ✅ | ✅ | ❌ |
| Transactions | ✅ | ✅ | ✅ | ❌ |
| Zero Dependencies | ✅ | ✅ | ❌ | ✅ |
| Server Required | ❌ | ❌ | ✅ | ❌ |
| Embedded Use | ✅ | ✅ | ❌ | ✅ |

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🤝 Contributing

Contributions are welcome! For feature requests, bug reports, or code contributions, please open an issue or pull request on GitHub.

For complete examples of all features, check out [usage.py](usage.py).

---

**Note**: ZenithDB is primarily a learning and development tool. While it's suitable for small to medium applications, it's not recommended as a production database for high-traffic or mission-critical systems.