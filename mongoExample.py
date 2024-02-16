from pymongo import MongoClient
import certifi
import os

ca = certifi.where()
connection_string = os.getenv("MONGODB_CONNECTION_STRING")

client = MongoClient(connection_string, tlsCAFile=ca)
db = client.ExampleDb
collection = db.ExampleCollection
document = {"group": "7", "grade": 100}



result = collection.insert_one(document)

print(f"Inserted document with id: {result.inserted_id}")

client.close()
