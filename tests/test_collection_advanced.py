import os
import pytest
from zenithdb import Database, Query

@pytest.fixture
def db():
    """Create a test database."""
    db_path = "test_collection.db"
    if os.path.exists(db_path):
        os.remove(db_path)
    db = Database(db_path)
    yield db
    db.close()
    if os.path.exists(db_path):
        os.remove(db_path)

def test_document_validation(db):
    """Test document validation."""
    users = db.collection("users")
    
    def validator(doc):
        return isinstance(doc.get('age'), int) and doc['age'] >= 0
    
    users.set_validator(validator)
    
    # Test valid document
    user_id = users.insert({"name": "John", "age": 30})
    assert user_id is not None
    
    # Test invalid document
    with pytest.raises(ValueError):
        users.insert({"name": "John", "age": -1})
    
    # Test bulk insert with validation
    with pytest.raises(ValueError):
        users.insert_many([
            {"name": "Alice", "age": 25},
            {"name": "Bob", "age": -5}
        ])

def test_get_all(db):
    users = db.collection("users")
    for i in range(14):
        users.insert({
            "name": f"Alice Smith{i}",
            "age": 20 + i,
            "email": "alice@example.com",
            "tags": ["customer"],
            "address": {"city": "London", "country": "UK"}
        })
    assert len(users.all()) == 14
    users.delete_many({})
    assert len(users.all()) == 0

def test_count_all(db):
    users = db.collection("users")
    for i in range(29):
        users.insert({
            "name": f"Alice Smith{i}",
            "age": 20 + i,
            "email": "alice@example.com",
            "tags": ["customer"],
            "address": {"city": "London", "country": "UK"}
        })
    assert users.count() == 29
    users.delete_many({})
    assert users.count() == 0
def test_query_edge_cases(db):
    """Test edge cases in queries."""
    users = db.collection("users")
    
    # Test empty array fields
    users.insert({"name": "John", "tags": []})
    results = users.find({"tags": {"$contains": "any"}})
    assert len(results) == 0
    
    # Test null values
    users.insert({"name": "Alice", "age": None})
    results = users.find({"age": None})
    assert len(results) == 1
    
    # Test non-existent fields
    results = users.find({"non_existent": "value"})
    assert len(results) == 0

def test_complex_updates(db):
    """Test complex update operations."""
    users = db.collection("users")
    
    user_id = users.insert({
        "name": "John",
        "profile": {"age": 30, "score": 100},
        "tags": ["a", "b"]
    })
    
    # Test nested updates
    users.update(
        {"_id": user_id},
        {"$set": {
            "profile.age": 31,
            "tags.0": "c"
        }}
    )
    
    updated = users.find_one({"_id": user_id})
    assert updated["profile"]["age"] == 31
    assert updated["tags"][0] == "c"

def test_full_text_search(db):
    """Test full text search."""
    users = db.collection("users")
    users.insert({"name":"John Smith","age":30,"email":"john@example.com","tags":["customer","premium"]})
    users.insert({"name":"Jane Doe","age":25,"email":"jane@example.com","tags":["customer","trial"]})
    assert len(users.find({"*":{"$contains":"Smith"}})) == 1
    assert len(users.find({"*":{"$contains":"Doe"}})) == 1
    assert len(users.find({"*":{"$contains":"Smith Doe"}})) == 0