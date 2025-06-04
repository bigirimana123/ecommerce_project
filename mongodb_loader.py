# Import libraries necessary for mongodb_loader.py file

import json
from pymongo import MongoClient

# Connect to MongoDB on localhost
client = MongoClient("mongodb://localhost:27017/")

# Create/use the ecommerce database
db = client["ecommerce"]

# Clear existing collections to avoid duplicates
db.users.drop()
db.products.drop()
db.categories.drop()
db.transactions.drop()

# Helper function to load JSON data
def load_json(path):
    with open(path, "r") as f:
        return json.load(f)

# Load Users
users = load_json("data/users.json")
db.users.insert_many(users)

# Load Categories
categories = load_json("data/categories.json")
db.categories.insert_many(categories)

# Load Products
products = load_json("data/products.json")
db.products.insert_many(products)

# Load Transactions
transactions = load_json("data/transactions.json")
db.transactions.insert_many(transactions)

print("✅ MongoDB data loading complete.")
