# ZenithDB

ZenithDB is a powerful and flexible document-oriented database implemented in Python, offering MongoDB-like functionality with a clean and intuitive API. ZenithDB is built on top of SQLite.

## Features

- **Document-Oriented Storage**: Store and manage JSON-like documents with nested structures
- **Powerful Querying**: Support for both dictionary-style queries and a fluent Query builder
- **Indexing**: Create single and compound indexes for optimized query performance
- **Unique Constraints**: Enforce uniqueness on specific fields
- **Complex Queries**: Support for range queries, array operations, and nested field access
- **Aggregation**: Perform grouping and aggregation operations
- **Relationships**: Manage relationships between collections
- **CRUD Operations**: Full support for Create, Read, Update, and Delete operations

## Installation

```bash
pip install zenithdb
```

## Quick Start

```python
from zenithdb import Database, Query

# Initialize database
db = Database("mydb.db")
users = db.collection("users")

# Create indexes
db.create_index("users", "age")
db.create_index("users", "email", unique=True)

# Insert data
user = {
    "name": "John Doe",
    "age": 30,
    "email": "john@example.com",
    "tags": ["customer", "premium"]
}
user_id = users.insert(user)

# Query data
q = Query()
premium_users = users.find(
    (q.age >= 25) & (q.age <= 35) & q.tags.contains("premium")
)
```

## Advanced Usage

Check out [usage.py](usage.py) for a complete example demonstrating:
- Index creation and management
- Complex queries using both dict and Query builder syntax
- Aggregation operations
- Relationship handling between collections
- Batch operations
- And more!

## Query Examples

### Dictionary Style
```python
users.find({
    "age": {"$gte": 25, "$lte": 35},
    "tags": {"$contains": "premium"}
})
```

### Query Builder Style
```python
q = Query()
users.find((q.age >= 25) & (q.age <= 35) & q.tags.contains("premium"))
```

## Aggregations

```python
from zenithdb import AggregateFunction

# Calculate average age
avg_age = users.aggregate([{
    "group": {
        "field": None,
        "function": AggregateFunction.AVG,
        "target": "age",
        "alias": "avg_age"
    }
}])

# Count by country
country_counts = users.aggregate([{
    "group": {
        "field": "address.country",
        "function": AggregateFunction.COUNT,
        "alias": "count"
    }
}])
```

## Best Practices

1. Create indexes for frequently queried fields
2. Use compound indexes for multi-field queries
3. Add unique constraints where appropriate
4. Close the database connection when done
5. Use the Query builder for complex queries for better readability