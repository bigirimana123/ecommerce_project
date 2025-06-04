# Importing necessary package for spark_batch.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list  # This is the missing and collect_list is a Spark SQL function
from pyspark.sql.functions import explode, col, split, count, sum as _sum, to_date, month, year

# Initialize Spark
spark = SparkSession.builder \
    .appName("EcommerceBatchAnalysis") \
    .getOrCreate()

# -----------------------------
# Load JSON files as DataFrames
# -----------------------------
users = spark.read.option("multiline", "true").json("data/users.json")
transactions = spark.read.option("multiline", "true").json("data/transactions.json")

# -----------------------------
# Clean and prepare data
# -----------------------------
# Flatten embedded items from transactions
items = transactions.select(
    "transaction_id", "user_id", "timestamp", explode("items").alias("item")
).select(
    "transaction_id", "user_id", "timestamp",
    col("item.product_id").alias("product_id"),
    col("item.quantity").alias("quantity")
)

# -----------------------------
# Analysis 1: Product Recommendation (Bought X also Bought Y)
# -----------------------------
# Step 1: Create transactions with product lists
txn_products = items.groupBy("transaction_id") \
    .agg(collect_list("product_id").alias("product_list"))

# Step 2: Explode and pair each product with others in same txn
from pyspark.sql.functions import posexplode, array_except, lit

# Explode to get base product (This  relationship implies that multiple products were added to cart in the same session and purchased togethe)
exploded = txn_products.select("transaction_id", posexplode("product_list").alias("idx", "product_id"))

# Join with same transaction to find other products
cross_joined = exploded.alias("a").join(
    exploded.alias("b"),
    (col("a.transaction_id") == col("b.transaction_id")) & (col("a.product_id") != col("b.product_id"))
).select(
    col("a.product_id").alias("product_A"),
    col("b.product_id").alias("product_B")
)

recommendations = cross_joined.groupBy("product_A", "product_B").agg(count("*").alias("pair_count")) \
    .orderBy(col("pair_count").desc())

print("🔹 Top Product Pairs Bought Together")
recommendations.show(10, truncate=False)

# -----------------------------
# Analysis 2: Cohort Analysis
# -----------------------------
users = users.withColumn("registration_month", to_date("registration_date").substr(0, 7))
items = items.withColumn("txn_month", to_date("timestamp").substr(0, 7))

# Join to get registration month per user in each transaction
user_txn = items.join(users.select("user_id", "registration_month"), on="user_id")

# Aggregate spend by cohort month and txn month
spending = user_txn.groupBy("registration_month", "txn_month") \
    .agg(_sum("quantity").alias("items_bought"))

print("🔹 Cohort Spending Matrix")
spending.orderBy("registration_month", "txn_month").show(10, truncate=False)

# Save results for dashboard
recommendations.write.mode("overwrite").json("output/recommendations")
spending.write.mode("overwrite").json("output/cohorts")

print("✅ Spark batch processing complete.")
