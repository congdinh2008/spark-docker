"""
Utility functions for NYC Taxi data processing
Contains schema definitions, validation functions, and helper utilities
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, TimestampType, DateType, LongType
)
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from typing import List, Dict, Any
import os
from glob import glob

# Schema definitions
YELLOW_TAXI_SCHEMA = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", LongType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", LongType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("payment_type", LongType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("Airport_fee", DoubleType(), True),
    StructField("cbd_congestion_fee", DoubleType(), True)
])

GREEN_TAXI_SCHEMA = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("lpep_pickup_datetime", TimestampType(), True),
    StructField("lpep_dropoff_datetime", TimestampType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("RatecodeID", LongType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("passenger_count", LongType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("ehail_fee", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("payment_type", LongType(), True),
    StructField("trip_type", LongType(), True),
    StructField("congestion_surcharge", DoubleType(), True)
])

TAXI_ZONE_SCHEMA = StructType([
    StructField("LocationID", IntegerType(), True),
    StructField("Borough", StringType(), True),
    StructField("Zone", StringType(), True),
    StructField("service_zone", StringType(), True)
])

def get_file_paths(data_dir: str, pattern: str) -> List[str]:
    """
    Get list of file paths matching the pattern
    
    Args:
        data_dir: Base directory path
        pattern: File pattern to match (e.g., "yellow_tripdata_2025-*.parquet")
    
    Returns:
        List of file paths
    """
    search_pattern = os.path.join(data_dir, pattern)
    files = glob(search_pattern)
    return sorted(files)

def validate_schema(df: DataFrame, expected_schema: StructType) -> Dict[str, Any]:
    """
    Validate DataFrame schema against expected schema
    
    Args:
        df: Input DataFrame
        expected_schema: Expected schema
    
    Returns:
        Dictionary with validation results
    """
    actual_fields = {field.name: field.dataType for field in df.schema.fields}
    expected_fields = {field.name: field.dataType for field in expected_schema.fields}
    
    missing_fields = set(expected_fields.keys()) - set(actual_fields.keys())
    extra_fields = set(actual_fields.keys()) - set(expected_fields.keys())
    type_mismatches = {}
    
    for field_name in set(actual_fields.keys()) & set(expected_fields.keys()):
        if actual_fields[field_name] != expected_fields[field_name]:
            type_mismatches[field_name] = {
                'actual': actual_fields[field_name],
                'expected': expected_fields[field_name]
            }
    
    return {
        'is_valid': len(missing_fields) == 0 and len(extra_fields) == 0 and len(type_mismatches) == 0,
        'missing_fields': list(missing_fields),
        'extra_fields': list(extra_fields),
        'type_mismatches': type_mismatches
    }

def get_data_quality_report(df: DataFrame) -> Dict[str, Any]:
    """
    Generate comprehensive data quality report
    
    Args:
        df: Input DataFrame
    
    Returns:
        Dictionary with data quality metrics
    """
    total_rows = df.count()
    
    # Calculate null counts for each column
    null_counts = {}
    for col in df.columns:
        null_count = df.filter(F.col(col).isNull()).count()
        null_counts[col] = {
            'null_count': null_count,
            'null_percentage': (null_count / total_rows) * 100 if total_rows > 0 else 0
        }
    
    # Calculate duplicate count
    duplicate_count = total_rows - df.dropDuplicates().count()
    
    return {
        'total_rows': total_rows,
        'total_columns': len(df.columns),
        'duplicate_count': duplicate_count,
        'duplicate_percentage': (duplicate_count / total_rows) * 100 if total_rows > 0 else 0,
        'null_counts': null_counts
    }

def standardize_column_names(df: DataFrame, taxi_type: str) -> DataFrame:
    """
    Standardize column names across different taxi types
    
    Args:
        df: Input DataFrame
        taxi_type: Type of taxi data ('yellow' or 'green')
    
    Returns:
        DataFrame with standardized column names
    """
    if taxi_type.lower() == 'yellow':
        return df.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime") \
                 .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
    elif taxi_type.lower() == 'green':
        return df.withColumnRenamed("lpep_pickup_datetime", "pickup_datetime") \
                 .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")
    else:
        return df

def add_metadata_columns(df: DataFrame, taxi_type: str, file_path: str = None) -> DataFrame:
    """
    Add metadata columns to the DataFrame
    
    Args:
        df: Input DataFrame
        taxi_type: Type of taxi data ('yellow' or 'green')
        file_path: Optional file path for tracking data lineage
    
    Returns:
        DataFrame with added metadata columns
    """
    df_with_metadata = df.withColumn("taxi_type", F.lit(taxi_type)) \
                         .withColumn("processed_timestamp", F.current_timestamp())
    
    if file_path:
        df_with_metadata = df_with_metadata.withColumn("source_file", F.lit(file_path))
    
    return df_with_metadata

def print_schema_comparison(df: DataFrame, expected_schema: StructType, title: str = "Schema Comparison"):
    """
    Print a formatted comparison between actual and expected schemas
    
    Args:
        df: Input DataFrame
        expected_schema: Expected schema
        title: Title for the comparison output
    """
    print(f"\n{'='*60}")
    print(f"{title}")
    print(f"{'='*60}")
    
    validation_result = validate_schema(df, expected_schema)
    
    if validation_result['is_valid']:
        print("✅ Schema validation PASSED")
    else:
        print("❌ Schema validation FAILED")
        
        if validation_result['missing_fields']:
            print(f"\n🔴 Missing fields: {validation_result['missing_fields']}")
        
        if validation_result['extra_fields']:
            print(f"\n🟡 Extra fields: {validation_result['extra_fields']}")
        
        if validation_result['type_mismatches']:
            print(f"\n🟠 Type mismatches:")
            for field, types in validation_result['type_mismatches'].items():
                print(f"   {field}: expected {types['expected']}, got {types['actual']}")
    
    print(f"\nActual Schema ({len(df.schema.fields)} fields):")
    df.printSchema()

def print_data_quality_summary(df: DataFrame, title: str = "Data Quality Summary"):
    """
    Print a formatted data quality summary
    
    Args:
        df: Input DataFrame
        title: Title for the summary
    """
    quality_report = get_data_quality_report(df)
    
    print(f"\n{'='*60}")
    print(f"{title}")
    print(f"{'='*60}")
    print(f"📊 Total Rows: {quality_report['total_rows']:,}")
    print(f"📊 Total Columns: {quality_report['total_columns']}")
    print(f"🔄 Duplicate Rows: {quality_report['duplicate_count']:,} ({quality_report['duplicate_percentage']:.2f}%)")
    
    print(f"\n📋 Null Value Summary:")
    for col, info in quality_report['null_counts'].items():
        if info['null_count'] > 0:
            print(f"   {col}: {info['null_count']:,} ({info['null_percentage']:.2f}%)")
