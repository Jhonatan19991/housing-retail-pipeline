# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Data Deduplication
# MAGIC 
# MAGIC This notebook handles data deduplication in the Silver layer using optimized merge operations.
# MAGIC 
# MAGIC ## Features:
# MAGIC - Efficient deduplication strategies
# MAGIC - Optimized merge operations
# MAGIC - Data quality monitoring
# MAGIC - Performance optimizations
# MAGIC - Error handling and logging

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration and Setup

# COMMAND ----------

import logging
from datetime import datetime
from typing import Dict, Any, Optional
import json
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import DataFrame

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
CONFIG = {
    "storage": {
        "account": "jhonastorage23",
        "tiers": ["bronze", "silver", "gold"]
    },
    "deduplication": {
        "partition_key": "url",
        "order_key": "fecha",
        "batch_size": 10000,
        "optimize_write": True,
        "merge_strategy": "latest_wins"  # or "append_only"
    },
    "data_quality": {
        "min_records": 1,
        "max_duplicates_ratio": 0.5  # Alert if more than 50% duplicates
    }
}

# Generate ADLS paths
def get_adls_paths() -> Dict[str, str]:
    account = CONFIG["storage"]["account"]
    return {
        tier: f"abfss://{tier}@{account}.dfs.core.windows.net/"
        for tier in CONFIG["storage"]["tiers"]
    }

adls_paths = get_adls_paths()
bronze_adls = adls_paths["bronze"]
silver_adls = adls_paths["silver"]
gold_adls = adls_paths["gold"]

logger.info(f"ADLS paths initialized: {adls_paths}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Utility Functions

# COMMAND ----------

def get_current_timestamp() -> str:
    """Get current timestamp in standardized format."""
    return datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

def log_dataframe_info(df: DataFrame, name: str):
    """Log DataFrame information for monitoring."""
    logger.info(f"📊 {name} DataFrame Info:")
    logger.info(f"   - Rows: {df.count():,}")
    logger.info(f"   - Columns: {len(df.columns)}")
    logger.info(f"   - Schema: {df.schema}")

def validate_dataframe(df: DataFrame, name: str) -> bool:
    """Validate DataFrame has required columns and data."""
    required_cols = ["url", "fecha"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    
    if missing_cols:
        logger.error(f"❌ {name} missing required columns: {missing_cols}")
        return False
    
    if df.count() == 0:
        logger.error(f"❌ {name} is empty")
        return False
    
    logger.info(f"✅ {name} validation passed")
    return True

def optimize_dataframe(df: DataFrame) -> DataFrame:
    """Apply performance optimizations to DataFrame."""
    # Cache if DataFrame will be used multiple times
    if df.count() < 1000000:  # Cache if less than 1M rows
        df = df.cache()
        logger.info("📦 DataFrame cached for performance")
    
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Functions

# COMMAND ----------

def check_duplication_stats(df: DataFrame, partition_key: str = "url") -> Dict[str, Any]:
    """Check duplication statistics for the DataFrame."""
    logger.info(f"🔍 Checking duplication stats for partition key: {partition_key}")
    
    # Count total records
    total_records = df.count()
    
    # Count unique records by partition key
    unique_records = df.select(partition_key).distinct().count()
    
    # Count duplicates
    duplicate_records = total_records - unique_records
    duplicate_ratio = duplicate_records / total_records if total_records > 0 else 0
    
    # Get top duplicate keys
    duplicate_counts = df.groupBy(partition_key).count() \
                        .filter(f"count > 1") \
                        .orderBy("count", ascending=False) \
                        .limit(10)
    
    stats = {
        "total_records": total_records,
        "unique_records": unique_records,
        "duplicate_records": duplicate_records,
        "duplicate_ratio": duplicate_ratio,
        "top_duplicates": duplicate_counts.collect()
    }
    
    logger.info(f"📊 Duplication Stats:")
    logger.info(f"   - Total records: {total_records:,}")
    logger.info(f"   - Unique records: {unique_records:,}")
    logger.info(f"   - Duplicate records: {duplicate_records:,}")
    logger.info(f"   - Duplicate ratio: {duplicate_ratio:.2%}")
    
    # Alert if duplicate ratio is too high
    if duplicate_ratio > CONFIG["data_quality"]["max_duplicates_ratio"]:
        logger.warning(f"⚠️ High duplicate ratio detected: {duplicate_ratio:.2%}")
    
    return stats

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Management Functions

# COMMAND ----------

def create_deduplication_table():
    """Create the deduplication table if it doesn't exist."""
    logger.info("🔄 Creating deduplication table...")
    
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS housing_silver_deduplicate (
        id_house BIGINT GENERATED ALWAYS AS IDENTITY,
        fecha DATE,
        url STRING,
        titulo STRING,
        constructor STRING,
        item VARIANT,
        unidades ARRAY<VARIANT>,
        full_info VARIANT,
        direccion_principal STRING,
        direccion_asociadas STRING,
        lat_lon STRING,
        department STRING,
        city STRING,
        ingestion_timestamp TIMESTAMP,
        batch_id STRING
    ) USING DELTA 
    LOCATION 'abfss://silver@jhonastorage23.dfs.core.windows.net/housing_silver_deduplicate'
    PARTITIONED BY (department, city)
    """
    
    try:
        spark.sql(create_table_sql)
        logger.info("✅ Deduplication table created/verified")
        return True
    except Exception as e:
        logger.error(f"❌ Error creating table: {e}")
        return False

def optimize_table():
    """Optimize the deduplication table for better performance."""
    logger.info("🔄 Optimizing deduplication table...")
    
    try:
        # Optimize the table
        spark.sql("OPTIMIZE housing_silver_deduplicate")
        
        # Z-order by frequently queried columns
        spark.sql("""
        ALTER TABLE housing_silver_deduplicate 
        ZORDER BY (url, fecha, department, city)
        """)
        
        logger.info("✅ Table optimized successfully")
        return True
    except Exception as e:
        logger.error(f"❌ Error optimizing table: {e}")
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deduplication Functions

# COMMAND ----------

def deduplicate_data(df: DataFrame) -> DataFrame:
    """Deduplicate data using SQL window functions with optimization."""
    logger.info("🔄 Deduplicating data...")
    
    initial_count = df.count()
    
    # Create temporary view
    df.createOrReplaceTempView("updating_housing")
    
    # Optimized deduplication query with better performance
    dedup_query = """
    WITH deduplicated_data AS (
        SELECT 
            fecha,
            url,
            titulo,
            constructor,
            item,
            CASE 
                WHEN unidades IS NOT NULL THEN SPLIT(unidades, ',')
                ELSE ARRAY()
            END AS unidades,
            full_info,
            direccion_principal,
            direccion_asociadas,
            lat_lon,
            department,
            city,
            ROW_NUMBER() OVER (
                PARTITION BY url 
                ORDER BY fecha DESC, 
                         COALESCE(titulo, '') DESC,
                         COALESCE(constructor, '') DESC
            ) AS row_num
        FROM updating_housing
        WHERE url IS NOT NULL 
          AND url != ''
    )
    SELECT 
        fecha,
        url,
        titulo,
        constructor,
        item,
        unidades,
        full_info,
        direccion_principal,
        direccion_asociadas,
        lat_lon,
        department,
        city
    FROM deduplicated_data
    WHERE row_num = 1
    """
    
    try:
        deduplicated_df = spark.sql(dedup_query)
        final_count = deduplicated_df.count()
        
        removed_duplicates = initial_count - final_count
        logger.info(f"✅ Data deduplicated: {initial_count:,} -> {final_count:,} rows")
        logger.info(f"   - Removed {removed_duplicates:,} duplicates")
        
        return deduplicated_df
        
    except Exception as e:
        logger.error(f"❌ Deduplication failed: {e}")
        raise

def prepare_data_for_merge(df: DataFrame) -> DataFrame:
    """Prepare data for merge operation with proper formatting."""
    logger.info("🔄 Preparing data for merge...")
    
    # Add metadata columns
    df = df.withColumn("ingestion_timestamp", current_timestamp()) \
           .withColumn("batch_id", lit(get_current_timestamp()))
    
    # Ensure proper data types and handle nulls
    df = df.withColumn("fecha", to_date(col("fecha"))) \
           .withColumn("url", trim(col("url"))) \
           .withColumn("titulo", trim(col("titulo"))) \
           .withColumn("constructor", trim(col("constructor"))) \
           .withColumn("direccion_principal", trim(col("direccion_principal"))) \
           .withColumn("direccion_asociadas", trim(col("direccion_asociadas"))) \
           .withColumn("lat_lon", trim(col("lat_lon"))) \
           .withColumn("department", trim(col("department"))) \
           .withColumn("city", trim(col("city")))
    
    # Filter out invalid records
    df = df.filter(
        col("url").isNotNull() & 
        (col("url") != "") &
        col("fecha").isNotNull()
    )
    
    logger.info(f"✅ Data prepared for merge: {df.count()} records")
    return df

def merge_into_deduplication_table(df: DataFrame):
    """Merge data into the deduplication table with optimization."""
    logger.info("🔄 Merging data into deduplication table...")
    
    # Prepare data
    df = prepare_data_for_merge(df)
    
    # Create temporary view for merge
    df.createOrReplaceTempView("new_data")
    
    # Optimized merge query
    merge_query = """
    MERGE INTO housing_silver_deduplicate t
    USING new_data u
    ON t.url = u.url
    WHEN MATCHED THEN 
        UPDATE SET 
            t.fecha = u.fecha,
            t.titulo = u.titulo,
            t.constructor = u.constructor,
            t.item = u.item,
            t.unidades = u.unidades,
            t.full_info = u.full_info,
            t.direccion_principal = u.direccion_principal,
            t.direccion_asociadas = u.direccion_asociadas,
            t.lat_lon = u.lat_lon,
            t.department = u.department,
            t.city = u.city,
            t.ingestion_timestamp = u.ingestion_timestamp,
            t.batch_id = u.batch_id
    WHEN NOT MATCHED THEN 
        INSERT (
            fecha, url, titulo, constructor, item, unidades, full_info,
            direccion_principal, direccion_asociadas, lat_lon, department, city,
            ingestion_timestamp, batch_id
        )
        VALUES (
            u.fecha, u.url, u.titulo, u.constructor, u.item, u.unidades, u.full_info,
            u.direccion_principal, u.direccion_asociadas, u.lat_lon, u.department, u.city,
            u.ingestion_timestamp, u.batch_id
        )
    """
    
    try:
        # Execute merge
        spark.sql(merge_query)
        
        # Get merge statistics
        merge_stats = spark.sql("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT url) as unique_urls
            FROM housing_silver_deduplicate
            WHERE batch_id = '""" + get_current_timestamp() + """'
        """).collect()[0]
        
        logger.info(f"✅ Data merged successfully:")
        logger.info(f"   - Total records: {merge_stats['total_records']:,}")
        logger.info(f"   - Unique URLs: {merge_stats['unique_urls']:,}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Merge operation failed: {e}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Loading Functions

# COMMAND ----------

def load_incremental_data(input_path: str = None) -> DataFrame:
    """Load incremental data for deduplication."""
    logger.info("🔄 Loading incremental data...")
    
    if input_path is None:
        # Use widget parameter or default path
        try:
            input_path = dbutils.widgets.get("input_path")
        except:
            # Default to latest data
            from datetime import datetime
            now = datetime.now().strftime("%Y-%m-%d")
            input_path = f"{silver_adls}/data_{now}"
    
    logger.info(f"📁 Loading data from: {input_path}")
    
    try:
        # Load data with error handling
        df = spark.read.format("delta").load(input_path)
        
        if not validate_dataframe(df, "Incremental Data"):
            raise Exception("Incremental data validation failed")
        
        log_dataframe_info(df, "Incremental Data")
        
        # Check duplication stats
        dup_stats = check_duplication_stats(df)
        
        return df, dup_stats
        
    except Exception as e:
        logger.error(f"❌ Error loading data: {e}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Processing Functions

# COMMAND ----------

def process_deduplication(input_path: str = None) -> Dict[str, Any]:
    """Main function to process deduplication."""
    logger.info("🚀 Starting deduplication process...")
    
    try:
        # Create/verify table
        if not create_deduplication_table():
            raise Exception("Failed to create deduplication table")
        
        # Load data
        df, dup_stats = load_incremental_data(input_path)
        
        # Optimize DataFrame
        df = optimize_dataframe(df)
        
        # Deduplicate data
        deduplicated_df = deduplicate_data(df)
        
        # Merge into table
        merge_into_deduplication_table(deduplicated_df)
        
        # Optimize table
        optimize_table()
        
        # Final statistics
        final_stats = spark.sql("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT url) as unique_urls,
                COUNT(DISTINCT department) as departments,
                COUNT(DISTINCT city) as cities
            FROM housing_silver_deduplicate
        """).collect()[0]
        
        results = {
            "status": "success",
            "timestamp": get_current_timestamp(),
            "input_stats": dup_stats,
            "final_stats": {
                "total_records": final_stats["total_records"],
                "unique_urls": final_stats["unique_urls"],
                "departments": final_stats["departments"],
                "cities": final_stats["cities"]
            },
            "processing_info": {
                "input_path": input_path,
                "duplicates_removed": dup_stats["duplicate_records"]
            }
        }
        
        logger.info("✅ Deduplication process completed successfully")
        return results
        
    except Exception as e:
        logger.error(f"❌ Deduplication process failed: {e}")
        results = {
            "status": "failed",
            "timestamp": get_current_timestamp(),
            "error": str(e)
        }
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Execution

# COMMAND ----------

def main():
    """Main execution function with comprehensive error handling."""
    try:
        logger.info("🚀 Starting Silver layer deduplication...")
        
        # Get input path from widget or use default
        input_path = None
        try:
            input_path = dbutils.widgets.get("input_path")
            logger.info(f"📁 Using input path from widget: {input_path}")
        except:
            logger.info("📁 Using default input path")
        
        # Process deduplication
        results = process_deduplication(input_path)
        
        # Log final results
        logger.info(f"🏁 Silver layer deduplication completed with status: {results['status']}")
        
        return results
        
    except Exception as e:
        logger.error(f"❌ Silver layer deduplication failed: {e}")
        results = {
            "timestamp": get_current_timestamp(),
            "status": "failed",
            "error": str(e)
        }
        raise

# COMMAND ----------

# Execute main function
results = main()

# Display final results
display(results)

# Exit with results for downstream notebooks
dbutils.notebook.exit(results) 