# Importing necessary libraries for mongodb_queries.py file 

from pymongo import MongoClient
from pprint import pprint

client = MongoClient("mongodb://localhost:27017/")
db = client["ecommerce"]

# 1. Top-Selling Products (by quantity sold)
print("🔹 Top-Selling Products:")
pipeline1 = [
    {"$unwind": "$items"},
    {"$group": {
        "_id": "$items.product_id",
        "total_sold": {"$sum": "$items.quantity"}
    }},
    {"$sort": {"total_sold": -1}},
    {"$limit": 5}
]
for doc in db.transactions.aggregate(pipeline1):
    pprint(doc)

# 2. Revenue by Product Category
print("\n🔹 Revenue by Category:")
pipeline2 = [
    {"$unwind": "$items"},
    {"$lookup": {
        "from": "products",
        "localField": "items.product_id",
        "foreignField": "product_id",
        "as": "product_info"
    }},
    {"$unwind": "$product_info"},
    {"$group": {
        "_id": "$product_info.category_id",
        "total_revenue": {"$sum": "$items.subtotal"}
    }},
    {"$sort": {"total_revenue": -1}}
]
for doc in db.transactions.aggregate(pipeline2):
    pprint(doc)

# 3. User Segmentation by Purchase Frequency
print("\n🔹 User Segmentation (by purchase frequency):")
pipeline3 = [
    {"$group": {
        "_id": "$user_id",
        "num_purchases": {"$sum": 1}
    }},
    {"$bucket": {
        "groupBy": "$num_purchases",
        "boundaries": [0, 2, 5, 10, 100],
        "default": "10+",
        "output": {
            "num_users": {"$sum": 1}
        }
    }}
]
for doc in db.transactions.aggregate(pipeline3):
    pprint(doc)
