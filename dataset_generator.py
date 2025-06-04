# Importing libraries necessary for generating dataset
import json
import random
from faker import Faker
from datetime import datetime, timedelta
import os

fake = Faker()

# Ensure the output directory exists
output_dir = "data"
os.makedirs(output_dir, exist_ok=True)

# ---------------------------
# USERS
# ---------------------------
users = []
for i in range(200):
    user = {
        "user_id": f"user_{i:06}",
        "geo_data": {
            "city": fake.city(),
            "state": fake.state_abbr(),
            "country": "US"
        },
        "registration_date": fake.date_time_between(start_date='-180d', end_date='-90d').isoformat(),
        "last_active": fake.date_time_between(start_date='-90d', end_date='now').isoformat()
    }
    users.append(user)

# Save users to JSON
with open(f"{output_dir}/users.json", "w") as f:
    json.dump(users, f, indent=2)

# ---------------------------
# CATEGORIES & SUBCATEGORIES
# ---------------------------
categories = []
for i in range(10):
    category = {
        "category_id": f"cat_{i:03}",
        "name": fake.company(),
        "subcategories": [
            {
                "subcategory_id": f"sub_{i:03}_{j:02}",
                "name": fake.bs().title(),
                "profit_margin": round(random.uniform(0.1, 0.5), 2)
            } for j in range(3)
        ]
    }
    categories.append(category)

with open(f"{output_dir}/categories.json", "w") as f:
    json.dump(categories, f, indent=2)

# ---------------------------
# PRODUCTS
# ---------------------------
products = []
for i in range(500):
    cat = random.choice(categories)
    sub = random.choice(cat["subcategories"])
    base_price = round(random.uniform(10, 500), 2)
    product = {
        "product_id": f"prod_{i:05}",
        "name": fake.catch_phrase(),
        "category_id": cat["category_id"],#This structure (relationship) makes it easy to group products in Spark or the dashboard by subcategory
        "subcategory_id": sub["subcategory_id"],
        "base_price": base_price,
        "current_stock": random.randint(0, 100),
        "is_active": random.choice([True, False]),
        "price_history": [
            {"price": round(base_price * random.uniform(0.9, 1.2), 2), "date": fake.date_this_year().isoformat()},
            {"price": base_price, "date": fake.date_this_year().isoformat()}
        ],
        "creation_date": fake.date_time_this_year().isoformat()
    }
    products.append(product)

with open(f"{output_dir}/products.json", "w") as f:
    json.dump(products, f, indent=2)

# ---------------------------
# SESSIONS
# ---------------------------
sessions = []
for i in range(800):
    user = random.choice(users)
    start = fake.date_time_between(start_date='-90d', end_date='now')
    end = start + timedelta(minutes=random.randint(1, 30))
    session = {
        "session_id": f"sess_{fake.uuid4().replace('-', '')[:10]}",
        "user_id": user["user_id"],
        "start_time": start.isoformat(),
        "end_time": end.isoformat(),
        "duration_seconds": int((end - start).total_seconds()),
        "geo_data": {
            "city": user["geo_data"]["city"],
            "state": user["geo_data"]["state"],
            "country": "US",
            "ip_address": fake.ipv4()
        },
        "device_profile": {
            "type": random.choice(["mobile", "desktop", "tablet"]),
            "os": random.choice(["Windows", "macOS", "Linux", "iOS", "Android"]),
            "browser": random.choice(["Chrome", "Firefox", "Safari", "Edge"])
        },
        "viewed_products": [random.choice(products)["product_id"] for _ in range(random.randint(1, 5))],
        "page_views": [
            {
                "timestamp": (start + timedelta(seconds=random.randint(0, 1800))).isoformat(),
                "page_type": random.choice(["home", "category_listing", "product_detail", "cart"]),
                "product_id": None if random.random() < 0.4 else random.choice(products)["product_id"],
                "category_id": random.choice(categories)["category_id"],
                "view_duration": random.randint(10, 200)
            } for _ in range(random.randint(1, 4))
        ],
        "cart_contents": {},
        "conversion_status": random.choice(["converted", "abandoned"]),
        "referrer": random.choice(["search_engine", "ad_campaign", "email", "direct"])
    }
    # Populate cart if converted
    if session["conversion_status"] == "converted":
        for prod_id in session["viewed_products"]:
            session["cart_contents"][prod_id] = {
                "quantity": random.randint(1, 3),
                "price": round(random.uniform(10, 300), 2)
            }
    sessions.append(session)

with open(f"{output_dir}/sessions.json", "w") as f:
    json.dump(sessions, f, indent=2)

# ---------------------------
# TRANSACTIONS
# ---------------------------
transactions = []
for sess in sessions:
    if sess["conversion_status"] == "converted":
        items = []
        subtotal = 0
        for pid, item in sess["cart_contents"].items():
            subtotal += item["price"] * item["quantity"]
            items.append({
                "product_id": pid,
                "quantity": item["quantity"],
                "unit_price": item["price"],
                "subtotal": round(item["price"] * item["quantity"], 2)
            })
        discount = round(subtotal * random.uniform(0, 0.2), 2)
        total = round(subtotal - discount, 2)
        transactions.append({
            "transaction_id": f"txn_{fake.uuid4().replace('-', '')[:12]}",
            "session_id": sess["session_id"],
            "user_id": sess["user_id"],
            "timestamp": sess["end_time"],
            "items": items,
            "subtotal": round(subtotal, 2),
            "discount": discount,
            "total": total,
            "payment_method": random.choice(["credit_card", "paypal", "apple_pay"]),
            "status": random.choice(["completed", "shipped", "refunded"])
        })

with open(f"{output_dir}/transactions.json", "w") as f:
    json.dump(transactions, f, indent=2)

print("✅ Data generation complete! Files saved in the 'data' directory.")
