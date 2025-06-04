# clv_analysis.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, col
import pymongo
import happybase
import pandas as pd

# ---------------------------------
# STEP 1: Read Transactions from MongoDB
# ---------------------------------
mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["ecommerce"]
transactions = list(mongo_db["transactions"].find({}, {"user_id": 1, "total": 1, "_id": 0}))

# Convert to pandas DataFrame
txn_df = pd.DataFrame(transactions)

# Aggregate total spend per user
clv_df = txn_df.groupby("user_id").agg(total_spent=("total", "sum")).reset_index()

# ---------------------------------
# STEP 2: Read Session Counts from HBase
# ---------------------------------
hbase_conn = happybase.Connection('localhost', port=9090)
table = hbase_conn.table("user_sessions")

session_counts = {}
for key, _ in table.scan():
    user = key.decode().split("#")[0]
    session_counts[user] = session_counts.get(user, 0) + 1

# Convert to pandas
session_df = pd.DataFrame([
    {"user_id": user, "session_count": count}
    for user, count in session_counts.items()
])

# ---------------------------------
# STEP 3: Merge in Pandas, Convert to Spark (This relationship- joins sessions from HBase with transactions from MongoDB, via user_id)
# ---------------------------------
merged_df = pd.merge(clv_df, session_df, on="user_id", how="inner")

# Calculate average CLV per session
merged_df["clv_per_session"] = merged_df["total_spent"] / merged_df["session_count"]

# Spark session
#spark = SparkSession.builder.appName("CLVAnalysis").getOrCreate()
spark = SparkSession.builder \
    .appName("CLV Analysis") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
    .getOrCreate()

# Convert to Spark DataFrame
spark_df = spark.createDataFrame(merged_df)

# Show final CLV values
print("🔹 Customer Lifetime Value per Session:")
spark_df.orderBy(col("clv_per_session").desc()).show(10, truncate=False)

# Save to disk for dashboard
spark_df.write.mode("overwrite").json("output/clv")

print("✅ CLV computation and export complete.")
