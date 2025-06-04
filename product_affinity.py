# product_affinity.py
"""
PRODUCT AFFINITY ANALYSIS
- Business Question: What products are often browsed or bought together?
- Data Sources: MongoDB (transactions), HBase (session product views)
- Output: Top N product pairs with highest co-occurrence
"""

import json
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, count

# ✅ Initialize Spark
spark = SparkSession.builder \
    .appName("ProductAffinity") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

# ✅ Load transaction data from MongoDB
client = MongoClient("mongodb://localhost:27017")
db = client["ecommerce"]
raw_txns = list(db.transactions.find({}, {"_id": 0, "transaction_id": 1, "items.product_id": 1}))

# ✅ Convert to Spark DataFrame
spark_df = spark.read.json(spark.sparkContext.parallelize(raw_txns))
spark_df = spark_df.withColumn("product", explode(col("items.product_id")))
spark_df.createOrReplaceTempView("exploded_txn")

# ✅ Self-join to find co-occurring products in same transaction
cooccur_df = spark.sql("""
    SELECT a.product AS product_A, b.product AS product_B, COUNT(*) AS pair_count
    FROM exploded_txn a
    JOIN exploded_txn b
      ON a.transaction_id = b.transaction_id AND a.product < b.product
    GROUP BY product_A, product_B
    ORDER BY pair_count DESC
    LIMIT 15
""")

# ✅ Show and export
cooccur_df.show(truncate=False)
cooccur_df.coalesce(1).write.mode("overwrite").json("output/recommendations")

spark.stop()
