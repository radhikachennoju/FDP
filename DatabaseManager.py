# DatabaseManager.py

from pymongo import MongoClient
from pprint import pprint
import sys

# --- Configuration ---
MONGO_URI = 'mongodb://localhost:27017/'
DATABASE_NAME = 'experiment_11_db'
COLLECTION_NAME = 'products'
DOCUMENT_SKU = "PRD-400" # Identifier for the document we will manipulate

# 1. CONNECT TO MONGODB
try:
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    products_collection = db[COLLECTION_NAME]
    # Clear the collection for a clean start
    products_collection.delete_many({})
    print(f"--- Connected to MongoDB. Collection '{COLLECTION_NAME}' cleared. ---")
    print("-" * 60)
except Exception as e:
    print(f"Error connecting to MongoDB: {e}")
    sys.exit(1)

# Helper function to display the current state of our target document
def display_target_document(sku):
    document = products_collection.find_one({"sku": sku})
    print(f"\n--- Current State of Document (SKU: {sku}) ---")
    if document:
        pprint(document)
    else:
        print("Document not found.")
    print("-" * 60)
    return document

# --------------------------------------------------------------------------------
# STEP 1: Insert a document with an array (CREATE)
# --------------------------------------------------------------------------------
print("===== STEP 1: Insert a document with an array =====")

item_document = {
    "name": "Bluetooth Speaker",
    "sku": DOCUMENT_SKU,
    "features": ["Waterproof", "Portable", "Long Battery"], # Simple array
    "connectors": [
        {"type": "Aux", "version": "3.5mm"},
        {"type": "USB", "version": "Type-C"}
    ], # Array of embedded documents
    "price": 49.99
}

insert_result = products_collection.insert_one(item_document)
print(f"Insertion successful. Document ID: {insert_result.inserted_id}")
display_target_document(DOCUMENT_SKU)


# --------------------------------------------------------------------------------
# STEP 2: Retrieve the document (READ)
# --------------------------------------------------------------------------------
print("===== STEP 2: Retrieve the document =====")

retrieved_doc = products_collection.find_one({"sku": DOCUMENT_SKU})
print("Document retrieved successfully:")
pprint(retrieved_doc)
print("-" * 60)


# --------------------------------------------------------------------------------
# STEP 3: Update the array (Add a new element) (UPDATE)
# --------------------------------------------------------------------------------
# Using $push to add a new feature to the 'features' array
print("===== STEP 3: Update the array (Add 'Voice Assistant' using $push) =====")

update_result = products_collection.update_one(
    {"sku": DOCUMENT_SKU},
    {"$push": {"features": "Voice Assistant"}} # Adds a new element
)

print(f"Update successful. Documents modified: {update_result.modified_count}")
display_target_document(DOCUMENT_SKU)


# --------------------------------------------------------------------------------
# STEP 4: Remove an element from the array (DELETE from array)
# --------------------------------------------------------------------------------
# Using $pull to remove a specific feature ('Portable') from the 'features' array
print("===== STEP 4: Remove an element from the array (Remove 'Portable' using $pull) =====")

update_result_remove = products_collection.update_one(
    {"sku": DOCUMENT_SKU},
    {"$pull": {"features": "Portable"}} # Removes the element
)

print(f"Removal successful. Documents modified: {update_result_remove.modified_count}")
display_target_document(DOCUMENT_SKU)


# --------------------------------------------------------------------------------
# STEP 5: Query using array filters (READ with filter)
# --------------------------------------------------------------------------------
print("===== STEP 5: Query using array filters =====")

# Query 5a: Find documents that contain both "Waterproof" AND "Long Battery"
print("\n--- 5a: Query using $all (Check for multiple features) ---")
query_all = products_collection.find(
    {"features": {"$all": ["Waterproof", "Long Battery"]}}
)
count_5a = products_collection.count_documents({"features": {"$all": ["Waterproof", "Long Battery"]}})
print(f"Found {count_5a} document(s) matching the $all query:")
for doc in query_all:
    pprint(doc)

# Query 5b: Find documents with a specific embedded document value (Type-C connector)
print("\n--- 5b: Query on embedded array (Check for Type-C connector) ---")
query_embedded = products_collection.find(
    {"connectors.version": "Type-C"}
)
count_5b = products_collection.count_documents({"connectors.version": "Type-C"})
print(f"Found {count_5b} document(s) matching the embedded query:")
for doc in query_embedded:
    pprint(doc)

# --- Close connection ---
client.close()
