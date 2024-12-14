from zenithdb import Database
from zenithdb.core.emitter import Emitter

def on_insert(event, data):
    print(f"Event: {event} => {data}")

# Create an emitter and subscribe to insert events
emitter = Emitter()
emitter.subscribe(on_insert)

# Create the database, passing the emitter
db = Database(db_path="mydb.sqlite", emitter=emitter)

# Insert a document
doc_id = db.insert("users", {"name": "Alice", "age": 30})

db.close()