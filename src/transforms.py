"""
Data transformation functions for NYC Taxi data processing
Contains functions for data cleaning, feature engineering, and aggregations
"""

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, IntegerType
from typing import List, Dict, Any
import os

def clean_taxi_data(df: DataFrame, taxi_type: str) -> DataFrame:
    """
    Clean taxi data by removing invalid records and outliers
    
    Args:
        df: Input DataFrame
        taxi_type: Type of taxi data ('yellow' or 'green')
    
    Returns:
        Cleaned DataFrame
    """
    print(f"🧹 Cleaning {taxi_type} taxi data...")
    
    # Store original count
    original_count = df.count()
    
    # Basic data cleaning rules
    cleaned_df = df.filter(
        # Valid timestamps
        (F.col("pickup_datetime").isNotNull()) &
        (F.col("dropoff_datetime").isNotNull()) &
        (F.col("pickup_datetime") < F.col("dropoff_datetime")) &
        
        # Valid trip distance
        (F.col("trip_distance") >= 0) &
        (F.col("trip_distance") <= 1000) &  # Remove extremely long trips
        
        # Valid passenger count
        (F.col("passenger_count") >= 1) &
        (F.col("passenger_count") <= 8) &
        
        # Valid fare amount
        (F.col("fare_amount") >= 0) &
        (F.col("fare_amount") <= 1000) &  # Remove extremely expensive fares
        
        # Valid total amount
        (F.col("total_amount") >= 0) &
        (F.col("total_amount") <= 1000) &
        
        # Valid location IDs
        (F.col("PULocationID").isNotNull()) &
        (F.col("DOLocationID").isNotNull()) &
        (F.col("PULocationID") != F.col("DOLocationID"))  # Different pickup and dropoff
    )
    
    cleaned_count = cleaned_df.count()
    removed_count = original_count - cleaned_count
    
    print(f"   📊 Original records: {original_count:,}")
    print(f"   📊 Cleaned records: {cleaned_count:,}")
    print(f"   📊 Removed records: {removed_count:,} ({(removed_count/original_count)*100:.2f}%)")
    
    return cleaned_df

def add_derived_features(df: DataFrame) -> DataFrame:
    """
    Add derived features to the taxi data
    
    Args:
        df: Input DataFrame
    
    Returns:
        DataFrame with additional features
    """
    print("🔧 Adding derived features...")
    
    # Calculate trip duration in minutes
    df_with_features = df.withColumn(
        "trip_duration_minutes",
        (F.unix_timestamp(F.col("dropoff_datetime")) - F.unix_timestamp(F.col("pickup_datetime"))) / 60
    )
    
    # Calculate average speed (mph)
    df_with_features = df_with_features.withColumn(
        "average_speed_mph",
        F.when(F.col("trip_duration_minutes") > 0,
               (F.col("trip_distance") / F.col("trip_duration_minutes")) * 60
        ).otherwise(0)
    )
    
    # Extract time-based features
    df_with_features = df_with_features.withColumn("pickup_hour", F.hour(F.col("pickup_datetime"))) \
                                       .withColumn("pickup_day_of_week", F.dayofweek(F.col("pickup_datetime"))) \
                                       .withColumn("pickup_month", F.month(F.col("pickup_datetime"))) \
                                       .withColumn("pickup_year", F.year(F.col("pickup_datetime")))
    
    # Add time period categories
    df_with_features = df_with_features.withColumn(
        "time_period",
        F.when((F.col("pickup_hour") >= 6) & (F.col("pickup_hour") < 12), "Morning")
         .when((F.col("pickup_hour") >= 12) & (F.col("pickup_hour") < 18), "Afternoon")
         .when((F.col("pickup_hour") >= 18) & (F.col("pickup_hour") < 22), "Evening")
         .otherwise("Night")
    )
    
    # Add weekend indicator
    df_with_features = df_with_features.withColumn(
        "is_weekend",
        F.when(F.col("pickup_day_of_week").isin([1, 7]), True).otherwise(False)
    )
    
    # Calculate tip rate (tip as percentage of fare)
    df_with_features = df_with_features.withColumn(
        "tip_rate",
        F.when(F.col("fare_amount") > 0,
               (F.col("tip_amount") / F.col("fare_amount")) * 100
        ).otherwise(0)
    )
    
    print("   ✅ Added features: trip_duration_minutes, average_speed_mph, time-based features")
    
    return df_with_features

def remove_outliers(df: DataFrame, columns: List[str], method: str = "iqr") -> DataFrame:
    """
    Remove outliers from specified columns
    
    Args:
        df: Input DataFrame
        columns: List of column names to check for outliers
        method: Method for outlier detection ('iqr' or 'zscore')
    
    Returns:
        DataFrame with outliers removed
    """
    print(f"🎯 Removing outliers using {method.upper()} method...")
    
    original_count = df.count()
    result_df = df
    
    if method.lower() == "iqr":
        for col in columns:
            # Calculate quartiles
            quantiles = result_df.select(
                F.expr(f"percentile_approx({col}, 0.25)").alias("q1"),
                F.expr(f"percentile_approx({col}, 0.75)").alias("q3")
            ).collect()[0]
            
            q1, q3 = quantiles[0], quantiles[1]
            iqr = q3 - q1
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            
            # Filter outliers
            result_df = result_df.filter(
                (F.col(col) >= lower_bound) & (F.col(col) <= upper_bound)
            )
            
            print(f"   📊 {col}: removed outliers outside [{lower_bound:.2f}, {upper_bound:.2f}]")
    
    final_count = result_df.count()
    removed_count = original_count - final_count
    
    print(f"   📊 Total outliers removed: {removed_count:,} ({(removed_count/original_count)*100:.2f}%)")
    
    return result_df

def aggregate_by_zone(df: DataFrame, zone_lookup_df: DataFrame) -> DataFrame:
    """
    Aggregate trip data by pickup zone
    
    Args:
        df: Trip data DataFrame
        zone_lookup_df: Zone lookup DataFrame
    
    Returns:
        Aggregated DataFrame by zone
    """
    print("📊 Aggregating data by pickup zone...")
    
    # Join with zone lookup
    df_with_zones = df.join(
        zone_lookup_df.withColumnRenamed("LocationID", "PULocationID")
                      .withColumnRenamed("Borough", "pickup_borough")
                      .withColumnRenamed("Zone", "pickup_zone"),
        "PULocationID",
        "left"
    )
    
    # Aggregate by zone
    zone_aggregates = df_with_zones.groupBy(
        "PULocationID", "pickup_borough", "pickup_zone"
    ).agg(
        F.count("*").alias("total_trips"),
        F.avg("trip_distance").alias("avg_trip_distance"),
        F.avg("trip_duration_minutes").alias("avg_trip_duration"),
        F.avg("fare_amount").alias("avg_fare_amount"),
        F.avg("total_amount").alias("avg_total_amount"),
        F.avg("tip_amount").alias("avg_tip_amount"),
        F.avg("tip_rate").alias("avg_tip_rate"),
        F.avg("passenger_count").alias("avg_passenger_count"),
        F.sum("total_amount").alias("total_revenue")
    ).orderBy(F.desc("total_trips"))
    
    print(f"   ✅ Created zone-level aggregations")
    
    return zone_aggregates

def aggregate_by_time(df: DataFrame) -> DataFrame:
    """
    Aggregate trip data by time periods
    
    Args:
        df: Trip data DataFrame
    
    Returns:
        Aggregated DataFrame by time periods
    """
    print("📊 Aggregating data by time periods...")
    
    time_aggregates = df.groupBy(
        "pickup_year", "pickup_month", "pickup_hour", "time_period", "is_weekend"
    ).agg(
        F.count("*").alias("total_trips"),
        F.avg("trip_distance").alias("avg_trip_distance"),
        F.avg("trip_duration_minutes").alias("avg_trip_duration"),
        F.avg("fare_amount").alias("avg_fare_amount"),
        F.avg("total_amount").alias("avg_total_amount"),
        F.avg("passenger_count").alias("avg_passenger_count"),
        F.sum("total_amount").alias("total_revenue")
    ).orderBy("pickup_year", "pickup_month", "pickup_hour")
    
    print(f"   ✅ Created time-based aggregations")
    
    return time_aggregates

def create_summary_statistics(df: DataFrame, taxi_type: str) -> Dict[str, Any]:
    """
    Create comprehensive summary statistics
    
    Args:
        df: Input DataFrame
        taxi_type: Type of taxi data
    
    Returns:
        Dictionary with summary statistics
    """
    print(f"📈 Creating summary statistics for {taxi_type} taxi data...")
    
    # Basic statistics
    total_trips = df.count()
    
    # Numeric column statistics
    numeric_cols = ["trip_distance", "trip_duration_minutes", "fare_amount", 
                   "total_amount", "tip_amount", "passenger_count"]
    
    stats = {}
    for col in numeric_cols:
        col_stats = df.select(
            F.mean(col).alias("mean"),
            F.stddev(col).alias("stddev"),
            F.min(col).alias("min"),
            F.max(col).alias("max"),
            F.expr(f"percentile_approx({col}, 0.5)").alias("median")
        ).collect()[0]
        
        stats[col] = {
            "mean": float(col_stats["mean"]) if col_stats["mean"] else 0,
            "stddev": float(col_stats["stddev"]) if col_stats["stddev"] else 0,
            "min": float(col_stats["min"]) if col_stats["min"] else 0,
            "max": float(col_stats["max"]) if col_stats["max"] else 0,
            "median": float(col_stats["median"]) if col_stats["median"] else 0
        }
    
    # Trip patterns
    busiest_hour = df.groupBy("pickup_hour").count().orderBy(F.desc("count")).first()["pickup_hour"]
    busiest_day = df.groupBy("pickup_day_of_week").count().orderBy(F.desc("count")).first()["pickup_day_of_week"]
    
    # Revenue statistics
    total_revenue = df.agg(F.sum("total_amount")).collect()[0][0]
    
    summary = {
        "taxi_type": taxi_type,
        "total_trips": total_trips,
        "total_revenue": float(total_revenue) if total_revenue else 0,
        "busiest_hour": busiest_hour,
        "busiest_day": busiest_day,
        "column_statistics": stats
    }
    
    print(f"   ✅ Summary statistics created")
    
    return summary

def save_to_parquet(df: DataFrame, output_path: str, mode: str = "overwrite"):
    """
    Save DataFrame to Parquet format
    
    Args:
        df: DataFrame to save
        output_path: Output path
        mode: Save mode ('overwrite', 'append')
    """
    print(f"💾 Saving data to {output_path}...")
    
    df.coalesce(1).write.mode(mode).parquet(output_path)
    
    print(f"   ✅ Data saved successfully")

def load_from_parquet(spark: SparkSession, input_path: str) -> DataFrame:
    """
    Load DataFrame from Parquet format
    
    Args:
        spark: SparkSession
        input_path: Input path
    
    Returns:
        Loaded DataFrame
    """
    print(f"📂 Loading data from {input_path}...")
    
    df = spark.read.parquet(input_path)
    
    print(f"   ✅ Data loaded successfully ({df.count():,} records)")
    
    return df
