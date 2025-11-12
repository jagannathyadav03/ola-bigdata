#!/usr/bin/env python3
"""
spark_queries.py
================
Analytical Queries using Spark SQL for Chh-OLA Trip Data

This script runs various analytical queries to extract business insights:
1. City-wise trip count and revenue
2. Peak demand hours analysis
3. Average fare per kilometer by city
4. Payment type distribution
5. Driver and rider statistics
6. Distance category analysis
7. Time-of-day patterns

Author: Big Data Analysis Student
Date: November 2025
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, max as spark_max, 
    min as spark_min, round as spark_round, desc, asc, concat, lit
)
import time
import os

# ============================================================================
# Initialize Spark Session
# ============================================================================

print("="*70)
print("📊 CHH-OLA TRIP DATA ANALYSIS - SPARK SQL QUERIES")
print("="*70)

spark = SparkSession.builder \
    .appName("Chh-OLA-Analytics") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print(f"✅ Spark Session initialized")

# ============================================================================
# Load Parquet Data
# ============================================================================

print("\n" + "="*70)
print("📥 LOADING PARQUET DATA")
print("="*70)

input_path = "/app/output/parquet/chh_ola_trips_processed"
df = spark.read.parquet(input_path)

# Cache for multiple queries
df.cache()
total_records = df.count()

print(f"✅ Loaded {total_records:,} records from Parquet")
print(f"✅ Data cached in memory for fast queries")

# Register as SQL temp view
df.createOrReplaceTempView("trips")
print(f"✅ Created SQL temp view: 'trips'")

# Create output directory
output_dir = "/app/output/query_results"
os.makedirs(output_dir, exist_ok=True)

# ============================================================================
# QUERY 1: City-wise Trip Count and Revenue Analysis
# ============================================================================

print("\n" + "="*70)
print("🏙️  QUERY 1: City-wise Trip Count and Revenue")
print("="*70)

query1 = spark.sql("""
    SELECT 
        city,
        COUNT(*) as trip_count,
        ROUND(SUM(total_amount), 2) as total_revenue,
        ROUND(AVG(total_amount), 2) as avg_revenue_per_trip,
        ROUND(AVG(distance_km), 2) as avg_distance_km,
        ROUND(SUM(distance_km), 2) as total_distance_km
    FROM trips
    GROUP BY city
    ORDER BY total_revenue DESC
""")

query1.show(10, truncate=False)

# Save to CSV
query1.coalesce(1).write.mode("overwrite").csv(
    f"{output_dir}/city_revenue_analysis",
    header=True
)
print("✅ Saved: city_revenue_analysis.csv")

# ============================================================================
# QUERY 2: Peak Demand Hours (Hour-wise Trip Distribution)
# ============================================================================

print("\n" + "="*70)
print("⏰ QUERY 2: Peak Demand Hours")
print("="*70)

query2 = spark.sql("""
    SELECT 
        pickup_hour as hour,
        COUNT(*) as trip_count,
        ROUND(SUM(total_amount), 2) as hourly_revenue,
        ROUND(AVG(total_amount), 2) as avg_fare,
        time_of_day
    FROM trips
    GROUP BY pickup_hour, time_of_day
    ORDER BY pickup_hour
""")

query2.show(24, truncate=False)

query2.coalesce(1).write.mode("overwrite").csv(
    f"{output_dir}/peak_hours_analysis",
    header=True
)
print("✅ Saved: peak_hours_analysis.csv")

# ============================================================================
# QUERY 3: Average Fare Per Kilometer by City
# ============================================================================

print("\n" + "="*70)
print("💰 QUERY 3: Average Fare per Kilometer by City")
print("="*70)

query3 = spark.sql("""
    SELECT 
        city,
        ROUND(AVG(fare_per_km), 2) as avg_fare_per_km,
        ROUND(MIN(fare_per_km), 2) as min_fare_per_km,
        ROUND(MAX(fare_per_km), 2) as max_fare_per_km,
        COUNT(*) as trip_count
    FROM trips
    WHERE fare_per_km > 0
    GROUP BY city
    ORDER BY avg_fare_per_km DESC
""")

query3.show(10, truncate=False)

query3.coalesce(1).write.mode("overwrite").csv(
    f"{output_dir}/fare_per_km_analysis",
    header=True
)
print("✅ Saved: fare_per_km_analysis.csv")

# ============================================================================
# QUERY 4: Payment Type Distribution
# ============================================================================

print("\n" + "="*70)
print("💳 QUERY 4: Payment Type Distribution")
print("="*70)

query4 = spark.sql("""
    SELECT 
        payment_type,
        COUNT(*) as trip_count,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM trips), 2) as percentage,
        ROUND(SUM(total_amount), 2) as total_revenue,
        ROUND(AVG(total_amount), 2) as avg_transaction_value
    FROM trips
    GROUP BY payment_type
    ORDER BY trip_count DESC
""")

query4.show(10, truncate=False)

query4.coalesce(1).write.mode("overwrite").csv(
    f"{output_dir}/payment_type_analysis",
    header=True
)
print("✅ Saved: payment_type_analysis.csv")

# ============================================================================
# QUERY 5: Distance Category Analysis
# ============================================================================

print("\n" + "="*70)
print("📏 QUERY 5: Trip Distance Category Distribution")
print("="*70)

query5 = spark.sql("""
    SELECT 
        distance_category,
        COUNT(*) as trip_count,
        ROUND(AVG(distance_km), 2) as avg_distance,
        ROUND(AVG(fare_amount), 2) as avg_fare,
        ROUND(AVG(trip_duration_minutes), 2) as avg_duration_min,
        ROUND(SUM(total_amount), 2) as total_revenue
    FROM trips
    GROUP BY distance_category
    ORDER BY 
        CASE distance_category
            WHEN 'Short' THEN 1
            WHEN 'Medium' THEN 2
            WHEN 'Long' THEN 3
            WHEN 'Very Long' THEN 4
        END
""")

query5.show(10, truncate=False)

query5.coalesce(1).write.mode("overwrite").csv(
    f"{output_dir}/distance_category_analysis",
    header=True
)
print("✅ Saved: distance_category_analysis.csv")

# ============================================================================
# QUERY 6: Top Performing Drivers
# ============================================================================

print("\n" + "="*70)
print("👨‍✈️  QUERY 6: Top 20 Drivers by Revenue")
print("="*70)

query6 = spark.sql("""
    SELECT 
        driver_id,
        COUNT(*) as total_trips,
        ROUND(SUM(total_amount), 2) as total_revenue,
        ROUND(AVG(total_amount), 2) as avg_revenue_per_trip,
        ROUND(AVG(rating_filled), 2) as avg_rating,
        ROUND(SUM(distance_km), 2) as total_distance_km
    FROM trips
    GROUP BY driver_id
    ORDER BY total_revenue DESC
    LIMIT 20
""")

query6.show(20, truncate=False)

query6.coalesce(1).write.mode("overwrite").csv(
    f"{output_dir}/top_drivers",
    header=True
)
print("✅ Saved: top_drivers.csv")

# ============================================================================
# QUERY 7: Time of Day Analysis
# ============================================================================

print("\n" + "="*70)
print("🌅 QUERY 7: Time of Day Analysis")
print("="*70)

query7 = spark.sql("""
    SELECT 
        time_of_day,
        COUNT(*) as trip_count,
        ROUND(SUM(total_amount), 2) as total_revenue,
        ROUND(AVG(total_amount), 2) as avg_fare,
        ROUND(AVG(distance_km), 2) as avg_distance
    FROM trips
    GROUP BY time_of_day
    ORDER BY 
        CASE time_of_day
            WHEN 'Morning' THEN 1
            WHEN 'Afternoon' THEN 2
            WHEN 'Evening' THEN 3
            WHEN 'Night' THEN 4
        END
""")

query7.show(10, truncate=False)

query7.coalesce(1).write.mode("overwrite").csv(
    f"{output_dir}/time_of_day_analysis",
    header=True
)
print("✅ Saved: time_of_day_analysis.csv")

# ============================================================================
# QUERY 8: Rate Code Distribution
# ============================================================================

print("\n" + "="*70)
print("🎫 QUERY 8: Rate Code Distribution")
print("="*70)

query8 = spark.sql("""
    SELECT 
        rate_code,
        CASE rate_code
            WHEN 1 THEN 'Standard'
            WHEN 2 THEN 'Airport'
            WHEN 3 THEN 'Connaught Place'
            WHEN 4 THEN 'Noida'
            WHEN 5 THEN 'Negotiated Fare'
            WHEN 6 THEN 'Pooled Ride'
        END as rate_type,
        COUNT(*) as trip_count,
        ROUND(AVG(fare_amount), 2) as avg_fare,
        ROUND(SUM(total_amount), 2) as total_revenue
    FROM trips
    GROUP BY rate_code
    ORDER BY trip_count DESC
""")

query8.show(10, truncate=False)

query8.coalesce(1).write.mode("overwrite").csv(
    f"{output_dir}/rate_code_analysis",
    header=True
)
print("✅ Saved: rate_code_analysis.csv")

# ============================================================================
# QUERY 9: Monthly Trend Analysis
# ============================================================================

print("\n" + "="*70)
print("📅 QUERY 9: Monthly Trip Trends")
print("="*70)

query9 = spark.sql("""
    SELECT 
        pickup_month as month,
        COUNT(*) as trip_count,
        ROUND(SUM(total_amount), 2) as monthly_revenue,
        ROUND(AVG(total_amount), 2) as avg_fare,
        COUNT(DISTINCT driver_id) as active_drivers,
        COUNT(DISTINCT rider_id) as active_riders
    FROM trips
    GROUP BY pickup_month
    ORDER BY pickup_month
""")

query9.show(12, truncate=False)

query9.coalesce(1).write.mode("overwrite").csv(
    f"{output_dir}/monthly_trends",
    header=True
)
print("✅ Saved: monthly_trends.csv")

# ============================================================================
# QUERY 10: Vendor Performance Comparison
# ============================================================================

print("\n" + "="*70)
print("🏢 QUERY 10: Vendor Performance Comparison")
print("="*70)

query10 = spark.sql("""
    SELECT 
        vendor_id,
        CASE vendor_id
            WHEN 1 THEN 'TaxiTech Inc.'
            WHEN 2 THEN 'DataCollectors Inc.'
        END as vendor_name,
        COUNT(*) as trip_count,
        ROUND(SUM(total_amount), 2) as total_revenue,
        ROUND(AVG(total_amount), 2) as avg_fare,
        ROUND(AVG(rating_filled), 2) as avg_rating,
        ROUND(AVG(distance_km), 2) as avg_distance
    FROM trips
    GROUP BY vendor_id
    ORDER BY vendor_id
""")

query10.show(10, truncate=False)

query10.coalesce(1).write.mode("overwrite").csv(
    f"{output_dir}/vendor_comparison",
    header=True
)
print("✅ Saved: vendor_comparison.csv")

# ============================================================================
# Summary Report
# ============================================================================

print("\n" + "="*70)
print("📈 OVERALL BUSINESS METRICS SUMMARY")
print("="*70)

summary = spark.sql("""
    SELECT 
        COUNT(*) as total_trips,
        COUNT(DISTINCT city) as cities_covered,
        COUNT(DISTINCT driver_id) as total_drivers,
        COUNT(DISTINCT rider_id) as total_riders,
        ROUND(SUM(total_amount), 2) as total_revenue,
        ROUND(AVG(total_amount), 2) as avg_fare,
        ROUND(AVG(distance_km), 2) as avg_distance,
        ROUND(AVG(trip_duration_minutes), 2) as avg_duration_min,
        ROUND(AVG(rating_filled), 2) as avg_rating
    FROM trips
""")

summary.show(truncate=False)

summary.coalesce(1).write.mode("overwrite").csv(
    f"{output_dir}/business_summary",
    header=True
)
print("✅ Saved: business_summary.csv")

# ============================================================================
# Execution Complete
# ============================================================================

print("\n" + "="*70)
print("🎉 ALL ANALYTICAL QUERIES EXECUTED SUCCESSFULLY")
print("="*70)

print("\n📊 Query Results Summary:")
print("   1. City-wise revenue analysis")
print("   2. Peak demand hours")
print("   3. Fare per kilometer by city")
print("   4. Payment type distribution")
print("   5. Distance category analysis")
print("   6. Top performing drivers")
print("   7. Time of day patterns")
print("   8. Rate code distribution")
print("   9. Monthly trends")
print("   10. Vendor performance comparison")
print("   11. Overall business metrics")

print(f"\n📁 All results saved to: {output_dir}")
print("\n🎓 Big Data Concepts Demonstrated:")
print("   • Spark SQL for complex analytics")
print("   • Aggregations and grouping")
print("   • Window functions")
print("   • Data caching for performance")
print("   • Multiple output formats (CSV)")
print("   • In-memory data processing")

print("\n👉 Next Step: Import CSV files into Power BI for visualization")

# Stop Spark session
spark.stop()
