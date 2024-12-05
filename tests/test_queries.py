import os
import pytest
from zenithdb import Database, Query

@pytest.fixture
def db():
    """Create a test database."""
    db_path = "test_queries.db"
    if os.path.exists(db_path):
        os.remove(db_path)
    db = Database(db_path)
    yield db
    db.close()
    if os.path.exists(db_path):
        os.remove(db_path)

def test_nested_field_queries(db):
    """Test queries with nested fields."""
    users = db.collection("users")
    
    # Insert test data
    test_data = [
        {
            "name": "John",
            "profile": {
                "age": 30,
                "location": {
                    "city": "New York",
                    "country": "USA"
                }
            },
            "tags": ["premium"]
        },
        {
            "name": "Alice",
            "profile": {
                "age": 25,
                "location": {
                    "city": "London",
                    "country": "UK"
                }
            },
            "tags": ["basic"]
        }
    ]
    users.insert_many(test_data)
    
    # Test deep nested field query
    results = users.find({
        "profile.location.country": "USA",
        "tags": {"$contains": "premium"}
    })
    assert len(results) == 1
    assert results[0]["name"] == "John"
    
    # Test Query builder with nested fields
    q = Query()
    results = users.find(
        (q.profile.location.country == "USA") & 
        (q.profile.age >= 25)
    )
    assert len(results) == 1
    assert results[0]["name"] == "John"

def test_array_operations(db):
    """Test array field operations."""
    users = db.collection("users")
    
    test_data = [
        {"name": "John", "tags": ["a", "b", "c"]},
        {"name": "Alice", "tags": ["b", "c", "d"]},
        {"name": "Bob", "tags": ["c", "d", "e"]}
    ]
    users.insert_many(test_data)
    
    # Test contains
    results = users.find({"tags": {"$contains": "a"}})
    assert len(results) == 1
    assert results[0]["name"] == "John"
    
    # Test multiple contains
    results = users.find({
        "tags": {"$contains": "c"},
        "name": {"$contains": "o"}
    })
    assert len(results) == 2

def test_comparison_operators(db):
    """Test comparison operators."""
    users = db.collection("users")
    
    test_data = [
        {"name": "John", "age": 30, "score": 100},
        {"name": "Alice", "age": 25, "score": 150},
        {"name": "Bob", "age": 35, "score": 90}
    ]
    users.insert_many(test_data)
    
    # Test greater than
    results = users.find({"age": {"$gt": 30}})
    assert len(results) == 1
    assert results[0]["name"] == "Bob"
    
    # Test between
    q = Query()
    results = users.find(
        (q.score >= 90) & (q.score <= 150)
    )
    assert len(results) == 3
    
    # Test not equal
    results = users.find({"age": {"$ne": 30}})
    assert len(results) == 2

def test_complex_combinations(db):
    """Test complex query combinations."""
    users = db.collection("users")
    
    test_data = [
        {
            "name": "John",
            "age": 30,
            "tags": ["premium"],
            "profile": {"level": "gold"}
        },
        {
            "name": "Alice",
            "age": 25,
            "tags": ["basic"],
            "profile": {"level": "silver"}
        }
    ]
    users.insert_many(test_data)
    
    # Test complex AND condition
    q = Query()
    results = users.find(
        (q.age >= 25) & 
        (q.tags.contains("premium")) & 
        (q.profile.level == "gold")
    )
    assert len(results) == 1
    assert results[0]["name"] == "John"