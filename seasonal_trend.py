# seasonal_trend.py
"""
SEASONAL TREND ANALYSIS
- Business Question: Are there patterns in transactions across months or weekdays?
- Data Source: MongoDB (transactions with timestamps)
- Output: Aggregated transaction volume over time
"""

from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, date_format, count

# ✅ Initialize Spark
spark = SparkSession.builder \
    .appName("SeasonalTrend") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

# ✅ Load transaction data
client = MongoClient("mongodb://localhost:27017")
db = client["ecommerce"]
txns = list(db.transactions.find({}, {"_id": 0, "timestamp": 1}))

# ✅ Create Spark DataFrame
txn_df = spark.read.json(spark.sparkContext.parallelize(txns))
txn_df = txn_df.withColumn("txn_date", to_date("timestamp"))

# ✅ Extract month and weekday
txn_df = txn_df.withColumn("month", date_format("txn_date", "yyyy-MM"))
txn_df = txn_df.withColumn("weekday", date_format("txn_date", "E"))

# ✅ Aggregate by month and weekday
monthly_df = txn_df.groupBy("month").agg(count("timestamp").alias("txn_count"))
weekday_df = txn_df.groupBy("weekday").agg(count("timestamp").alias("txn_count"))

# ✅ Save output for dashboard
monthly_df.coalesce(1).write.mode("overwrite").json("output/seasonal/monthly")
weekday_df.coalesce(1).write.mode("overwrite").json("output/seasonal/weekday")

monthly_df.show()
weekday_df.show()

spark.stop()
