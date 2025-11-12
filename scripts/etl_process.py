#!/usr/bin/env python3
"""
etl_process.py
==============
ETL Pipeline: Extract, Transform, Load for Chh-OLA Trip Data

This script demonstrates key Big Data concepts:
1. Reading CSV data with PySpark
2. Data cleaning and transformation
3. Partitioning strategy
4. Caching for performance
5. Writing to Parquet (columnar storage)

Author: Big Data Analysis Student
Date: November 2025

FIXES:
- Fixed data quality check syntax error
- Simplified null value checking
- All Big Data concepts still demonstrated


"""


from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, dayofmonth, hour, 
    dayofweek, when, round as spark_round,
    to_timestamp, datediff, unix_timestamp, sum as spark_sum
)
from pyspark.sql.types import DoubleType
import time
import os

# ============================================================================
# STEP 1: Initialize Spark Session
# ============================================================================

print("="*70)
print("🚀 INITIALIZING SPARK SESSION")
print("="*70)

spark = SparkSession.builder \
    .appName("Chh-OLA-ETL-Pipeline") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.default.parallelism", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print(f"✅ Spark Version: {spark.version}")
print(f"✅ Spark Master: {spark.sparkContext.master}")
print(f"✅ Available Cores: {spark.sparkContext.defaultParallelism}")

# ============================================================================
# STEP 2: Extract - Read CSV Data
# ============================================================================

print("\n" + "="*70)
print("📥 EXTRACTING DATA FROM CSV")
print("="*70)

input_path = "/app/data/chh_ola_trips.csv"
start_time = time.time()

df_raw = spark.read.csv(
    input_path,
    header=True,
    inferSchema=True,
    timestampFormat="yyyy-MM-dd HH:mm:ss"
)

print(f"✅ Loaded {df_raw.count():,} records in {time.time() - start_time:.2f} seconds")
print(f"✅ Schema detected with {len(df_raw.columns)} columns")

print("\n📋 DATA SCHEMA:")
df_raw.printSchema()

print("\n📊 SAMPLE DATA (First 5 rows):")
df_raw.show(5, truncate=False)

# ============================================================================
# STEP 3: Transform - Data Cleaning and Feature Engineering
# ============================================================================

print("\n" + "="*70)
print("🔧 TRANSFORMING DATA")
print("="*70)

print("\n📅 Converting timestamp columns...")
df_transformed = df_raw \
    .withColumn("pickup_time", to_timestamp(col("pickup_time"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("drop_time", to_timestamp(col("drop_time"), "yyyy-MM-dd HH:mm:ss"))

print("⏰ Creating time-based features...")
df_transformed = df_transformed \
    .withColumn("pickup_year", year(col("pickup_time"))) \
    .withColumn("pickup_month", month(col("pickup_time"))) \
    .withColumn("pickup_day", dayofmonth(col("pickup_time"))) \
    .withColumn("pickup_hour", hour(col("pickup_time"))) \
    .withColumn("pickup_dayofweek", dayofweek(col("pickup_time"))) \
    .withColumn("trip_duration_minutes", 
                (unix_timestamp(col("drop_time")) - unix_timestamp(col("pickup_time"))) / 60)

print("💰 Calculating fare metrics...")
df_transformed = df_transformed \
    .withColumn("fare_per_km", 
                when(col("distance_km") > 0, 
                     spark_round(col("fare_amount") / col("distance_km"), 2))
                .otherwise(0))

print("📏 Categorizing trip distances...")
df_transformed = df_transformed \
    .withColumn("distance_category",
                when(col("distance_km") <= 5, "Short")
                .when((col("distance_km") > 5) & (col("distance_km") <= 15), "Medium")
                .when((col("distance_km") > 15) & (col("distance_km") <= 30), "Long")
                .otherwise("Very Long"))

print("🌅 Categorizing time of day...")
df_transformed = df_transformed \
    .withColumn("time_of_day",
                when((col("pickup_hour") >= 5) & (col("pickup_hour") < 12), "Morning")
                .when((col("pickup_hour") >= 12) & (col("pickup_hour") < 17), "Afternoon")
                .when((col("pickup_hour") >= 17) & (col("pickup_hour") < 21), "Evening")
                .otherwise("Night"))

print("⭐ Handling missing ratings...")
avg_rating = df_transformed.agg({"rating": "avg"}).collect()[0][0]
df_transformed = df_transformed \
    .withColumn("rating_filled", 
                when(col("rating").isNull(), round(avg_rating, 1))
                .otherwise(col("rating")))

print("🧹 Cleaning invalid records...")
initial_count = df_transformed.count()
df_clean = df_transformed \
    .filter(col("fare_amount") > 0) \
    .filter(col("distance_km") > 0) \
    .filter(col("total_amount") > 0) \
    .filter(col("trip_duration_minutes") > 0)

removed_count = initial_count - df_clean.count()
print(f"✅ Removed {removed_count} invalid records")
print(f"✅ Clean dataset: {df_clean.count():,} records")

# ============================================================================
# STEP 4: Caching for Performance
# ============================================================================

print("\n" + "="*70)
print("⚡ CACHING DATA FOR PERFORMANCE")
print("="*70)

df_clean.cache()
cached_count = df_clean.count()

print(f"✅ Cached {cached_count:,} records in memory")
print("📝 Big Data Concept: Caching stores intermediate results in memory")
print("   for faster access during iterative queries")

# ============================================================================
# STEP 5: Repartitioning
# ============================================================================

print("\n" + "="*70)
print("🔀 REPARTITIONING DATA")
print("="*70)

print(f"📊 Original partitions: {df_clean.rdd.getNumPartitions()}")

df_partitioned = df_clean.repartition(8, "city")
print(f"📊 After repartitioning by city: {df_partitioned.rdd.getNumPartitions()}")
print("📝 Big Data Concept: Repartitioning distributes data across partitions")
print("   for parallel processing and better query performance")

# ============================================================================
# STEP 6: Load - Write to Parquet Format
# ============================================================================

print("\n" + "="*70)
print("💾 LOADING DATA TO PARQUET FORMAT")
print("="*70)

output_path = "/app/output/parquet/chh_ola_trips_processed"
start_time = time.time()

df_partitioned.write \
    .mode("overwrite") \
    .partitionBy("city") \
    .parquet(output_path)

write_time = time.time() - start_time

print(f"✅ Data written to Parquet in {write_time:.2f} seconds")
print(f"✅ Output location: {output_path}")
print("📝 Big Data Concept: Parquet is a columnar storage format that:")
print("   - Compresses data efficiently")
print("   - Enables fast column-based queries")
print("   - Supports schema evolution")
print("   - Provides better performance for analytical queries")

# ============================================================================
# STEP 7: Verification - Read Parquet and Display Stats
# ============================================================================

print("\n" + "="*70)
print("✅ VERIFYING PARQUET OUTPUT")
print("="*70)

df_parquet = spark.read.parquet(output_path)

print(f"✅ Parquet file contains {df_parquet.count():,} records")

print("\n📋 FINAL SCHEMA:")
df_parquet.printSchema()

print("\n📊 SAMPLE TRANSFORMED DATA:")
df_parquet.select(
    "trip_id", "city", "distance_km", "fare_per_km", 
    "distance_category", "time_of_day", "pickup_hour"
).show(10)

# ============================================================================
# STEP 8: Data Quality Checks (FIXED)
# ============================================================================

print("\n" + "="*70)
print("🔍 DATA QUALITY CHECKS")
print("="*70)

print("\n1️⃣ Null Value Summary:")
for col_name in ["trip_id", "city", "fare_amount", "distance_km"]:
    null_count = df_parquet.filter(col(col_name).isNull()).count()
    print(f"   {col_name}: {null_count} nulls")

print("\n2️⃣ City Distribution:")
df_parquet.groupBy("city").count().orderBy(col("count").desc()).show()

print("\n3️⃣ Distance Category Distribution:")
df_parquet.groupBy("distance_category").count().orderBy(col("count").desc()).show()

# ============================================================================
# STEP 9: Generate Summary Statistics
# ============================================================================

print("\n" + "="*70)
print("📈 SUMMARY STATISTICS")
print("="*70)

summary = df_parquet.select(
    "distance_km", "fare_amount", "total_amount", 
    "fare_per_km", "trip_duration_minutes"
).summary("count", "mean", "min", "max", "stddev")

summary.show()

# ============================================================================
# STEP 10: Cleanup and Close
# ============================================================================

print("\n" + "="*70)
print("🎉 ETL PIPELINE COMPLETED SUCCESSFULLY")
print("="*70)

print("\n📊 Pipeline Summary:")
print(f"   ✅ Input records: {initial_count:,}")
print(f"   ✅ Records processed: {cached_count:,}")
print(f"   ✅ Records removed: {removed_count}")
print(f"   ✅ Final records in Parquet: {df_parquet.count():,}")
print(f"   ✅ Output format: Parquet (partitioned by city)")
print(f"   ✅ Total execution time: {time.time() - start_time:.2f} seconds")

print("\n🎓 Big Data Concepts Demonstrated:")
print("   1. Distributed data processing with PySpark")
print("   2. Schema inference and data types")
print("   3. Data transformation and feature engineering")
print("   4. Caching for performance optimization")
print("   5. Repartitioning for parallel processing")
print("   6. Columnar storage with Parquet format")
print("   7. Partition-based file organization")
print("   8. Lazy evaluation and DAG execution")

print("\n👉 Next Step: Run spark_queries.py for analytical queries")

spark.stop()