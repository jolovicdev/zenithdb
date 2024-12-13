"""
ZenithDB Usage Example
"""

from zenithdb import Database, Query, AggregateFunction
import os

def main():
    # Cleanup any existing database
    if os.path.exists("example.db"):
        os.remove("example.db")

    print("\n=== Database Initialization ===")
    # Initialize with performance settings
    db = Database(
        "example.db",
        max_connections=10,  # Connection pool size
        debug=True  # Enable query plan analysis
    )

    print("\n=== Collection Management ===")
    users = db.collection("users")
    print(f"Collections count: {db.count_collections()}")
    print(f"All collections: {db.list_collections()}")

    print("\n=== Indexing ===")
    # Create indexes for better performance
    db.create_index("users", ["age"])
    db.create_index("users", ["profile.location.country", "age"])
    db.create_index("users", ["email"], unique=True)
    print("Created indexes:", db.list_indexes("users"))

    print("\n=== Document Validation ===")
    # Add validation for user documents
    def age_validator(doc):
        return isinstance(doc.get('age'), int) and doc['age'] >= 0
    users.set_validator(age_validator)
    print("Added age validator")

    print("\n=== Document Insertion ===")
    # Insert test data
    test_users = [
        {
            "name": "John Doe",
            "age": 30,
            "email": "john@example.com",
            "tags": ["premium", "tech"],
            "profile": {
                "location": {"city": "New York", "country": "USA"},
                "interests": ["coding", "music"]
            }
        },
        {
            "name": "Alice Smith",
            "age": 25,
            "email": "alice@example.com",
            "tags": ["basic"],
            "profile": {
                "location": {"city": "London", "country": "UK"},
                "interests": ["art", "travel"]
            }
        },
        {
            "name": "Bob Wilson",
            "age": 35,
            "email": "bob@example.com",
            "tags": ["premium", "finance"],
            "profile": {
                "location": {"city": "New York", "country": "USA"},
                "interests": ["stocks", "sports"]
            }
        }
    ]
    
    # Single insert
    user_id = users.insert(test_users[0])
    print(f"Inserted single user with ID: {user_id}")
    
    # Bulk insert
    users.insert_many(test_users[1:])
    print(f"Inserted {len(test_users)-1} more users")

    print("\n=== Query Styles ===")
    print("1. Find One:")
    user = users.find_one({"name": "John Doe"})
    print(f"Found user: {user['name']}")

    print("\n2. Dictionary Style Queries:")
    # All comparison operators
    results = users.find({
        "age": {"$gt": 25, "$lte": 35},  # Greater than, Less than or equal
        "tags": {"$contains": "premium"},  # Array contains
        "profile.location.city": "New York",  # Nested field
        "name": {"$ne": "Alice Smith"}  # Not equal
    })
    print("Users matching complex criteria:")
    for user in results:
        print(f"- {user['name']}, {user['age']}")

    print("\n3. Query Builder Style:")
    q = Query()
    results = users.find(
        (q.age > 25) &
        (q.age <= 35) &
        q.tags.contains("premium") &
        (q.profile.location.city == "New York")
    )
    print("Same query with Query builder:")
    for user in results:
        print(f"- {user['name']}, {user['age']}")

    print("\n4. Pagination and Sorting:")
    # Create query with sorting and pagination
    q = Query(collection="users", database=db)
    q.sort("age", ascending=False)  # Sort by age descending
    q.sort("name", ascending=True)  # Then by name ascending
    q.limit(2)  # Get 2 users per page
    q.skip(1)   # Skip first user
    
    paginated = q.execute()
    print("Paginated results (page 1, 2 users):")
    for user in paginated:
        print(f"- {user['name']}, {user['age']}")

    print("\n5. Full-text Search:")
    results = users.find({"*": {"$contains": "tech"}})
    print("Users with 'tech' anywhere in their document:")
    for user in results:
        print(f"- {user['name']}")

    print("\n6. Array Operations:")
    results = users.find({
        "tags": {"$contains": "premium"},
        "profile.interests": {"$contains": "coding"}
    })
    print("Users with specific array values:")
    for user in results:
        print(f"- {user['name']}: {user['tags']}")

    print("\n=== Relationships ===")
    # Create orders collection
    orders = db.collection("orders")
    db.create_index("orders", ["user_id"])  # Index for better join performance

    # Add orders for a user
    orders.insert_many([
        {
            "user_id": user_id,
            "product": "Laptop",
            "price": 1200.00,
            "status": "completed"
        },
        {
            "user_id": user_id,
            "product": "Mouse",
            "price": 25.00,
            "status": "pending"
        }
    ])

    # Find user's orders
    user_orders = orders.find({"user_id": user_id})
    print(f"\nOrders for user {user_id}:")
    for order in user_orders:
        print(f"- {order['product']}: ${order['price']}, Status: {order['status']}")

    print("\n=== Aggregations ===")
    # Multiple aggregation examples
    agg_result = users.aggregate([{
        "group": {
            "field": "profile.location.country",
            "function": AggregateFunction.AVG,
            "target": "age",
            "alias": "avg_age"
        }
    }])
    print("\nAverage age by country:")
    for result in agg_result:
        print(f"- {result['profile.location.country']}: {result['avg_age']:.1f}")

    # Count by multiple fields
    count_result = users.aggregate([{
        "group": {
            "field": "profile.location.country",
            "function": AggregateFunction.COUNT,
            "alias": "user_count"
        }
    }])
    print("\nUsers per country:")
    for result in count_result:
        print(f"- {result['profile.location.country']}: {result['user_count']}")

    print("\n=== Collection Operations ===")
    print("Collection contents:")
    users.print_collection()
    print(f"\nTotal documents: {users.count()}")
    print(f"Filtered count: {users.count({'age': {'$gt': 30}})}")

    print("\n=== Bulk Operations ===")
    bulk_ops = users.bulk_operations()
    
    # Bulk insert
    new_users = [
        {
            "name": "User1",
            "age": 31,
            "email": "user1@example.com",
            "tags": ["new"],
            "profile": {"location": {"city": "Boston", "country": "USA"}}
        },
        {
            "name": "User2",
            "age": 32,
            "email": "user2@example.com",
            "tags": ["new"],
            "profile": {"location": {"city": "Chicago", "country": "USA"}}
        }
    ]
    inserted_ids = bulk_ops.bulk_insert("users", new_users)
    print(f"Bulk inserted {len(inserted_ids)} users")

    # Bulk update
    updates = [
        {"_id": inserted_ids[0], "status": "active"},
        {"_id": inserted_ids[1], "status": "active"}
    ]
    bulk_ops.bulk_update("users", updates)
    print("Bulk updated user statuses")

    # Bulk delete
    bulk_ops.bulk_delete("users", inserted_ids)
    print("Bulk deleted users")

    print("\n=== Cleanup ===")
    # Drop specific collection
    db.drop_collection("orders")
    print("Dropped orders collection")

    # Drop all collections
    db.drop_all_collections()
    print("Dropped all collections")

    # Drop specific index
    db.drop_index("idx_users_email")
    print("Dropped email index")

    db.close()
    os.remove("example.db")
    print("Cleanup complete")

if __name__ == "__main__":
    main() 