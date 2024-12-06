"""
ZenithDB Usage Example
"""

import os
from zenithdb import Database, Query, AggregateFunction

def main():
    # Cleanup any existing database
    if os.path.exists("example.db"):
        os.remove("example.db")

    # Initialize database
    db = Database("example.db",debug=False,max_connections=10,max_result_size=10000)
    users = db.collection("users")
    orders = db.collection("orders")

    print("\n" + "="*50)
    print("Basic CRUD Operations")
    print("="*50)

    # Create indexes for better performance
    print("\nCreating Indexes...")
    db.create_index("users", "age")
    db.create_index("users", ["address.country", "age"])
    db.create_index("users", "email", unique=True)
    print("✓ Created indexes:", db.list_indexes("users"))

    # Insert a user
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
    user_id = users.insert(user)
    print("\n✓ Successfully inserted user:")
    print(f"  ID: {user_id}")

    # Add more users for demonstrating queries
    users.insert_many([
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
        },
        {
            "name": "Carol White",
            "age": 28,
            "email": "carol@example.com",
            "tags": ["customer", "premium"],
            "address": {"city": "New York", "country": "USA"}
        }
    ])

    # Find with dict query
    found_user = users.find_one({"_id": user_id})
    print("\nLookup by ID:")
    print(f"✓ Found user: {found_user['name']} ({found_user['email']})")

    # Find with Query builder (using compound index)
    q = Query()
    found_user = users.find_one(
        (q.age >= 25) & (q.age <= 35) & q.tags.contains("premium")
    )
    print("\nComplex Query Result:")
    print(f"✓ Found user: {found_user['name']} ({found_user['email']})")

    # Update user
    update_result = users.update(
        {"_id": user_id},
        {"$set": {"age": 31, "tags": ["customer", "premium", "updated"]}}
    )
    print("\nUpdate Operation:")
    print(f"✓ Success: {update_result}")
    updated_user = users.find_one({"_id": user_id})
    print(f"✓ Updated user data: {updated_user['name']}, Age: {updated_user['age']}, Tags: {updated_user['tags']}")

    print("\n" + "="*50)
    print("Complex Queries")
    print("="*50)
    
    # Query using index on age
    print("\nUsers aged 25-35 with premium tag (dict query):")
    premium_users = users.find({
        "age": {"$gte": 25, "$lte": 35},
        "tags": {"$contains": "premium"}
    })
    for user in premium_users:
        print(f"✓ {user['name']:<15} | Age: {user['age']:<3} | {user['email']}")

    # Same query with Query builder
    print("\nUsers aged 25-35 with premium tag (Query builder):")
    q = Query()
    premium_users = users.find(
        (q.age >= 25) & (q.age <= 35) & q.tags.contains("premium")
    )
    for user in premium_users:
        print(f"✓ {user['name']:<15} | Age: {user['age']:<3} | {user['email']}")

    print("\n" + "="*50)
    print("Aggregations")
    print("="*50)
    
    # Calculate average age
    avg_age = users.aggregate([{
        "group": {
            "field": None,
            "function": AggregateFunction.AVG,
            "target": "age",
            "alias": "avg_age"
        }
    }])
    print(f"\n✓ Average user age: {avg_age[0]['avg_age']:.1f} years")

    # Count users by country (using compound index)
    print("\nUser distribution by country:")
    country_counts = users.aggregate([{
        "group": {
            "field": "address.country",
            "function": AggregateFunction.COUNT,
            "alias": "count"
        }
    }])
    for country in ["France", "UK", "USA"]:
        count = next((c["count"] for c in country_counts 
                     if c["address.country"] == country), 0)
        print(f"✓ {country:<6}: {count} users")

    print("\n" + "="*50)
    print("Relationships")
    print("="*50)
    
    # Add some orders
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

    # Find orders for user (create index for better performance)
    db.create_index("orders", "user_id")
    print(f"\nOrders for user: {found_user['name']}")
    user_orders = orders.find({"user_id": user_id})
    for order in user_orders:
        print(f"✓ {order['product']:<10} | ${order['price']:<8.2f} | Status: {order['status']}")

    print("\n" + "="*50)
    print("Cleanup")
    print("="*50)
    db.close()
    os.remove("example.db")
    print("✓ Database cleaned up successfully")

if __name__ == "__main__":
    main() 