from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

# Create Spark session
spark = SparkSession.builder.appName("RetailDataPipeline").getOrCreate()

# Read data
df = spark.read.csv("data/retail_data.csv", header=True, inferSchema=True)

# Show initial data
print("Original Data:")
df.show()

# -------------------------------
# Data Cleaning
# -------------------------------
df_clean = df.dropna()

# Convert column to correct type (if needed)
df_clean = df_clean.withColumn("amount", col("amount").cast("int"))

# -------------------------------
# Transformation
# -------------------------------

# Filter high-value transactions
df_filtered = df_clean.filter(col("amount") > 5000)

# Aggregate total sales by city
df_grouped = df_filtered.groupBy("city").agg(_sum("amount").alias("total_sales"))

# -------------------------------
# Output
# -------------------------------
print("Transformed Data:")
df_grouped.show()

# Optional: Write output (simulation)
df_grouped.write.mode("overwrite").csv("output/processed_data")

# Stop session
spark.stop()
