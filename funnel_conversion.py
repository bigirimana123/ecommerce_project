# funnel_conversion.py
"""
FUNNEL CONVERSION ANALYSIS
- Business Question: How many users progress from product view → cart → purchase?
- Data Sources: HBase (session product views), MongoDB (cart + transaction)
- Output: Funnel counts by stage per user
"""

import happybase
import pandas as pd
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, countDistinct

# ✅ Start Spark
spark = SparkSession.builder \
    .appName("FunnelConversion") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

# ✅ 1. Load session views from HBase (simulate viewed product tracking)
connection = happybase.Connection("localhost", port=9090)
table = connection.table("user_sessions")

view_events = []
for key, data in table.scan():
    user_id = key.decode().split("#")[0]
    session_id = data.get(b"session_info:session_id", b"").decode()
    viewed = data.get(b"session_info:viewed_products", b"").decode().split(",") if b"session_info:viewed_products" in data else []
    for product_id in viewed:
        view_events.append((user_id, session_id, product_id.strip(), "view"))

view_df = pd.DataFrame(view_events, columns=["user_id", "session_id", "product_id", "event"])

# ✅ 2. Load cart and purchase from MongoDB
client = MongoClient("mongodb://localhost:27017")
db = client["ecommerce"]

cart_data = list(db.carts.find({}, {"_id": 0, "user_id": 1, "session_id": 1, "items.product_id": 1}))
purchase_data = list(db.transactions.find({}, {"_id": 0, "user_id": 1, "session_id": 1, "items.product_id": 1}))

cart_events = []
for entry in cart_data:
    for item in entry.get("items", []):
        cart_events.append((entry["user_id"], entry["session_id"], item["product_id"], "cart"))

purchase_events = []
for entry in purchase_data:
    for item in entry.get("items", []):
        purchase_events.append((entry["user_id"], entry["session_id"], item["product_id"], "purchase"))

# ✅ Combine all events
df_all = pd.DataFrame(view_events + cart_events + purchase_events, columns=["user_id", "session_id", "product_id", "event"])

# ✅ Convert to Spark DataFrame
spark_df = spark.createDataFrame(df_all)
spark_df.createOrReplaceTempView("events")

# ✅ Funnel stage counts per user
funnel_df = spark.sql("""
    SELECT event, COUNT(DISTINCT user_id) AS users
    FROM events
    GROUP BY event
""")

funnel_df.show()
funnel_df.coalesce(1).write.mode("overwrite").json("output/funnel")

spark.stop()
