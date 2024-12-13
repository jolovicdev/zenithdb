"""
Test suite for ZenithDB
"""

import pytest
import os
import time
from zenithdb import Database, Query, AggregateFunction
import sqlite3
DB_PATH = "test_PRE.db"
@pytest.fixture
def db():
    """Create a test database."""
    db_path = "test.db"
    if os.path.exists(db_path):
        os.remove(db_path)
    if os.path.exists(db_path + "-wal"):
        os.remove(db_path + "-wal")
    if os.path.exists(db_path + "-shm"):
        os.remove(db_path + "-shm")
    db = Database(db_path)
    yield db
    # Cleanup after tests
    db.close()
    if os.path.exists(db_path):
        os.remove(db_path)

@pytest.fixture
def users_collection(db):
    """Get users collection and ensure it's empty."""
    users = db.collection("users")
    users.delete_many({})
    return users

def test_basic_crud(users_collection):
    """Test basic CRUD operations."""
    # Insert
    user = {
        "name": "John Doe",
        "age": 30,
        "email": "john@example.com",
        "tags": ["customer", "premium"],
        "address": {
            "city": "San Francisco",
            "country": "USA"
        }
    }
    user_id = users_collection.insert(user)
    assert user_id is not None

    # Find by ID
    found = users_collection.find_one({"_id": user_id})
    assert found is not None
    assert found["name"] == "John Doe"
    assert found["age"] == 30
    assert "premium" in found["tags"]
    assert found["address"]["country"] == "USA"

    # Update
    update_result = users_collection.update(
        {"_id": user_id},
        {"$set": {"age": 31, "tags": ["customer", "premium", "updated"]}}
    )
    assert update_result == 1

    # Verify update
    updated = users_collection.find_one({"_id": user_id})
    assert updated["age"] == 31
    assert "updated" in updated["tags"]

    # Delete
    delete_result = users_collection.delete({"_id": user_id})
    assert delete_result == 1

    # Verify deletion
    assert users_collection.find_one({"_id": user_id}) is None

def test_bulk_operations(users_collection):
    """Test bulk insert operations."""
    users = [
        {
            "name": "Alice Smith",
            "age": 25,
            "email": "alice@example.com",
            "tags": ["customer"],
            "address": {"city": "London", "country": "UK"}
        },
        {
            "name": "Bob Johnson",
            "age": 35,
            "email": "bob@example.com",
            "tags": ["customer", "trial"],
            "address": {"city": "Paris", "country": "France"}
        }
    ]
    
    # Bulk insert
    ids = users_collection.insert_many(users)
    assert len(ids) == 2

    # Verify insertion
    all_users = list(users_collection.find({}))
    assert len(all_users) == 2

def test_complex_queries(users_collection):
    """Test complex query operations."""
    # Insert test data
    users = [
        {
            "name": "John Doe",
            "age": 30,
            "tags": ["premium", "customer"],
            "address": {"country": "USA"}
        },
        {
            "name": "Alice Smith",
            "age": 25,
            "tags": ["customer"],
            "address": {"country": "UK"}
        },
        {
            "name": "Bob Johnson",
            "age": 35,
            "tags": ["trial", "customer"],
            "address": {"country": "France"}
        }
    ]
    users_collection.insert_many(users)

    # Test range query with array contains
    results = list(users_collection.find({
        "age": {"$gte": 25, "$lte": 35},
        "tags": {"$contains": "premium"}
    }))
    assert len(results) == 1
    assert results[0]["name"] == "John Doe"

    # Test Query builder
    q = Query()
    results = list(users_collection.find(
        (q.age >= 25) & (q.age <= 35) & q.tags.contains("premium")
    ))
    assert len(results) == 1
    assert results[0]["name"] == "John Doe"

    # Test nested field query
    results = list(users_collection.find({
        "address.country": "USA",
        "tags": {"$contains": "premium"}
    }))
    assert len(results) == 1
    assert results[0]["name"] == "John Doe"

def test_aggregations(users_collection):
    """Test aggregation operations."""
    # Insert test data
    users = [
        {
            "name": "John Doe",
            "age": 30,
            "address": {"country": "USA"}
        },
        {
            "name": "Alice Smith",
            "age": 25,
            "address": {"country": "UK"}
        },
        {
            "name": "Bob Johnson",
            "age": 35,
            "address": {"country": "USA"}
        }
    ]
    users_collection.insert_many(users)

    # Test average age
    avg_age = users_collection.aggregate([{
        "group": {
            "field": None,
            "function": AggregateFunction.AVG,
            "target": "age",
            "alias": "avg_age"
        }
    }])
    assert len(avg_age) == 1
    assert avg_age[0]["avg_age"] == 30.0

    # Test count by country
    country_counts = users_collection.aggregate([{
        "group": {
            "field": "address.country",
            "function": AggregateFunction.COUNT,
            "alias": "count"
        }
    }])
    assert len(country_counts) == 2
    usa_count = next(c["count"] for c in country_counts 
                    if c["address.country"] == "USA")
    assert usa_count == 2

def test_indexes(users_collection):
    """Test index operations."""
    db = users_collection.database

    # Create indexes
    age_idx = db.create_index("users", "age")
    compound_idx = db.create_index("users", ["address.country", "age"])
    unique_idx = db.create_index("users", "email", unique=True)

    # List indexes
    indexes = db.list_indexes("users")
    assert len(indexes) == 3

    # Test unique constraint
    users_collection.insert({
        "name": "Test User",
        "email": "test@example.com",
        "age": 25
    })

    # Verify unique constraint
    with pytest.raises(sqlite3.IntegrityError):
        users_collection.insert({
            "name": "Another User",
            "email": "test@example.com",
            "age": 30
        })

def test_relationships(db):
    """Test relationships between collections."""
    users = db.collection("users")
    orders = db.collection("orders")

    # Create user
    user_id = users.insert({
        "name": "John Doe",
        "email": "john@example.com"
    })

    # Create orders for user
    orders.insert_many([
        {
            "user_id": user_id,
            "product": "Mouse",
            "price": 25.00,
            "status": "pending"
        },
        {
            "user_id": user_id,
            "product": "Laptop",
            "price": 1200.00,
            "status": "completed"
        }
    ])

    # Create index for better performance
    db.create_index("orders", "user_id")

    # Find orders for user
    user_orders = list(orders.find({"user_id": user_id}))
    assert len(user_orders) == 2
    assert user_orders[0]["product"] in ["Mouse", "Laptop"]
    assert user_orders[1]["product"] in ["Mouse", "Laptop"]

def test_performance(users_collection):
    """Test performance of various operations."""
    # Generate test data
    test_users = [
        {
            "name": f"User{i}",
            "age": i % 50 + 20,
            "email": f"user{i}@example.com",
            "tags": ["tag1", "tag2"] if i % 2 == 0 else ["tag3"],
            "address": {
                "city": f"City{i % 5}",
                "country": f"Country{i % 3}"
            }
        }
        for i in range(1000)
    ]

    # Test bulk insert performance
    start_time = time.time()
    users_collection.insert_many(test_users)
    insert_time = time.time() - start_time
    assert insert_time < 2.0, "Bulk insert took too long"

    # Create indexes for query performance
    db = users_collection.database
    db.create_index("users", "age")

    # Test query performance
    start_time = time.time()
    results = list(users_collection.find({
        "age": {"$gte": 25, "$lte": 35},
        "tags": {"$contains": "tag1"}
    }))
    query_time = time.time() - start_time
    assert query_time < 0.1, "Query took too long"

    # Test aggregation performance
    start_time = time.time()
    result = users_collection.aggregate([{
        "group": {
            "field": "address.country",
            "function": AggregateFunction.AVG,
            "target": "age",
            "alias": "avg_age"
        }
    }])
    agg_time = time.time() - start_time
    assert agg_time < 0.1, "Aggregation took too long"
    
def test_drop_all_collections(db):
    """Test dropping all collections."""
    users = db.collection("users")
    users.insert({"name":"John","age":20})
    users.insert({"name":"Jane","age":21})
    users.insert({"name":"Jim","age":22})
    db.drop_all_collections()
    assert db.list_collections() == []
def test_drop_collection(db):
    """Test dropping a collection."""
    users = db.collection("users")
    users.insert({"name":"John","age":20})
    users.insert({"name":"Jane","age":21})
    users.insert({"name":"Jim","age":22})
    db.drop_collection("users")
    assert db.list_collections() == []
def test_create_and_insert():
    # Ensure a clean start
    if os.path.exists("DB_PATH"):
        os.remove(DB_PATH)

    db = Database(DB_PATH)
    users = db.collection("users")
    
    user_id = users.insert({"name": "John Doe", "age": 30})
    db.close()

    # Don't remove the DB here; we want to test persistence in the next test.
    # At this point, the database file (and WAL/SHM files) still exist on disk,
    # simulating the program ending with data written.

def test_read_after_reopen():
    # Now simulate a "new run" of the program
    # We expect the data inserted in the previous test to still be there
    db = Database(DB_PATH)
    users = db.collection("users")

    found = users.find_one({"name": "John Doe"})
    assert found is not None
    assert found["age"] == 30

    # Cleanup after the test if needed
    db.close()
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    if os.path.exists(DB_PATH + "-wal"):
        os.remove(DB_PATH + "-wal")
    if os.path.exists(DB_PATH + "-shm"):
        os.remove(DB_PATH + "-shm")