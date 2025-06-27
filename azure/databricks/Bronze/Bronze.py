# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Data Ingestion
# MAGIC 
# MAGIC This notebook handles the initial data ingestion from various sources into the Bronze layer.
# MAGIC 
# MAGIC ## Features:
# MAGIC - Configuration management
# MAGIC - Error handling and logging
# MAGIC - Data quality checks
# MAGIC - Performance optimizations
# MAGIC - Modular functions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration and Setup

# COMMAND ----------

# Configuration management
import logging
from datetime import datetime
from typing import Dict, Any, Optional
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration dictionary
CONFIG = {
    "storage": {
        "account": "jhonastorage23",
        "tiers": ["bronze", "silver", "gold"]
    },
    "data_sources": {
        "csv": {
            "path": "data.csv",
            "options": {
                "multiline": "true",
                "header": "true",
                "inferSchema": "false"  # Better performance with explicit schema
            }
        },
        "overpass": {
            "url": "https://overpass-api.de/api/interpreter",
            "timeout": 600,
            "query_file": "overpass_query.txt"
        }
    },
    "processing": {
        "batch_size": 10000,
        "partition_columns": ["fecha"],
        "optimize_write": True
    }
}

# Generate ADLS paths
def get_adls_paths() -> Dict[str, str]:
    """Generate ADLS paths for all tiers."""
    account = CONFIG["storage"]["account"]
    return {
        tier: f"abfss://{tier}@{account}.dfs.core.windows.net/"
        for tier in CONFIG["storage"]["tiers"]
    }

# Initialize paths
adls_paths = get_adls_paths()
bronze_adls = adls_paths["bronze"]
silver_adls = adls_paths["silver"]
gold_adls = adls_paths["gold"]

logger.info(f"ADLS paths initialized: {adls_paths}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Utility Functions

# COMMAND ----------

def validate_storage_paths() -> bool:
    """Validate that all storage paths are accessible."""
    try:
        for tier, path in adls_paths.items():
            dbutils.fs.ls(path)
            logger.info(f"✅ {tier} path validated: {path}")
        return True
    except Exception as e:
        logger.error(f"❌ Storage path validation failed: {e}")
        return False

def get_current_timestamp() -> str:
    """Get current timestamp in standardized format."""
    return datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

def log_dataframe_info(df, name: str):
    """Log DataFrame information for monitoring."""
    logger.info(f"📊 {name} DataFrame Info:")
    logger.info(f"   - Rows: {df.count():,}")
    logger.info(f"   - Columns: {len(df.columns)}")
    logger.info(f"   - Schema: {df.schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Functions

# COMMAND ----------

def check_data_quality(df, name: str) -> Dict[str, Any]:
    """Perform data quality checks on DataFrame."""
    quality_report = {
        "total_rows": df.count(),
        "null_counts": {},
        "duplicate_rows": df.count() - df.dropDuplicates().count(),
        "empty_strings": {}
    }
    
    # Check null counts
    for col in df.columns:
        null_count = df.filter(f"{col} IS NULL").count()
        quality_report["null_counts"][col] = null_count
        
        # Check empty strings for string columns
        if df.schema[col].dataType.typeName() == "string":
            empty_count = df.filter(f"({col} = '') OR ({col} IS NULL)").count()
            quality_report["empty_strings"][col] = empty_count
    
    logger.info(f"🔍 Data Quality Report for {name}:")
    logger.info(f"   - Total rows: {quality_report['total_rows']:,}")
    logger.info(f"   - Duplicate rows: {quality_report['duplicate_rows']:,}")
    logger.info(f"   - Null counts: {quality_report['null_counts']}")
    
    return quality_report

# COMMAND ----------

# MAGIC %md
# MAGIC ## CSV Data Ingestion

# COMMAND ----------

def ingest_csv_data() -> Optional[Any]:
    """Ingest CSV data with error handling and optimization."""
    try:
        logger.info("🔄 Starting CSV data ingestion...")
        
        # Read CSV with optimized options
        csv_path = f"{bronze_adls}/{CONFIG['data_sources']['csv']['path']}"
        options = CONFIG['data_sources']['csv']['options']
        
        df = spark.read.options(**options).csv(csv_path)
        
        # Log initial info
        log_dataframe_info(df, "Raw CSV")
        
        # Data quality check
        quality_report = check_data_quality(df, "Raw CSV")
        
        # Add ingestion metadata
        df = df.withColumn("ingestion_timestamp", F.current_timestamp()) \
               .withColumn("source_file", F.lit(CONFIG['data_sources']['csv']['path'])) \
               .withColumn("batch_id", F.lit(get_current_timestamp()))
        
        # Optimized write to Silver
        write_options = {
            "format": "delta",
            "mode": "overwrite",
            "mergeSchema": "true"
        }
        
        if CONFIG["processing"]["optimize_write"]:
            write_options["optimizeWrite"] = "true"
        
        output_path = f"{silver_adls}/data_{get_current_timestamp()}"
        df.write.options(**write_options).save(output_path)
        
        logger.info(f"✅ CSV data successfully ingested to: {output_path}")
        
        return {
            "status": "success",
            "output_path": output_path,
            "quality_report": quality_report,
            "timestamp": get_current_timestamp()
        }
        
    except Exception as e:
        logger.error(f"❌ CSV ingestion failed: {e}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overpass API Data Ingestion

# COMMAND ----------

def get_overpass_query() -> str:
    """Get the Overpass query for POI data."""
    return """
[out:json][timeout:300];
// 1) Selecciona Colombia como área
area["ISO3166-1:alpha2"="CO"][admin_level=2]->.colombia;

// 2) Dentro de esa área, busca nodos y ways de interés
(
  node["amenity"~"hospital|school"](area.colombia);
  way ["amenity"~"hospital|school"](area.colombia);
  node["shop"~"mall"](area.colombia);
  way ["shop"~"mall"](area.colombia);
  node["leisure"="park"](area.colombia);
  way ["leisure"="park"](area.colombia);
);

// 3) Devuelve geometría completa (nodos + ways) en JSON
out body;
>;
out skel qt;
"""

def ingest_overpass_data() -> Optional[Any]:
    """Ingest POI data from Overpass API with error handling."""
    try:
        logger.info("🔄 Starting Overpass API data ingestion...")
        
        import requests
        
        # Get query
        overpass_query = get_overpass_query()
        
        # Make API request with retry logic
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = requests.post(
                    CONFIG['data_sources']['overpass']['url'],
                    data={"data": overpass_query},
                    timeout=CONFIG['data_sources']['overpass']['timeout']
                )
                response.raise_for_status()
                break
            except requests.exceptions.RequestException as e:
                if attempt == max_retries - 1:
                    raise
                logger.warning(f"Attempt {attempt + 1} failed, retrying... Error: {e}")
                import time
                time.sleep(2 ** attempt)  # Exponential backoff
        
        # Save to Bronze layer
        output_path = f"{bronze_adls}/pois_colombia_{get_current_timestamp()}.json"
        dbutils.fs.put(output_path, response.text, overwrite=True)
        
        # Also save latest version
        latest_path = f"{bronze_adls}/pois_colombia.json"
        dbutils.fs.put(latest_path, response.text, overwrite=True)
        
        logger.info(f"✅ Overpass data successfully ingested to: {output_path}")
        
        return {
            "status": "success",
            "output_path": output_path,
            "latest_path": latest_path,
            "response_size": len(response.text),
            "timestamp": get_current_timestamp()
        }
        
    except Exception as e:
        logger.error(f"❌ Overpass ingestion failed: {e}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Execution

# COMMAND ----------

def main():
    """Main execution function with comprehensive error handling."""
    try:
        logger.info("🚀 Starting Bronze layer data ingestion...")
        
        # Validate storage paths
        if not validate_storage_paths():
            raise Exception("Storage path validation failed")
        
        # Initialize results
        results = {
            "timestamp": get_current_timestamp(),
            "csv_ingestion": None,
            "overpass_ingestion": None,
            "overall_status": "success"
        }
        
        # Ingest CSV data
        try:
            results["csv_ingestion"] = ingest_csv_data()
        except Exception as e:
            logger.error(f"CSV ingestion failed: {e}")
            results["csv_ingestion"] = {"status": "failed", "error": str(e)}
            results["overall_status"] = "partial_failure"
        
        # Ingest Overpass data
        try:
            results["overpass_ingestion"] = ingest_overpass_data()
        except Exception as e:
            logger.error(f"Overpass ingestion failed: {e}")
            results["overpass_ingestion"] = {"status": "failed", "error": str(e)}
            results["overall_status"] = "partial_failure"
        
        # Log final results
        logger.info(f"🏁 Bronze layer ingestion completed with status: {results['overall_status']}")
        
        return results
        
    except Exception as e:
        logger.error(f"❌ Bronze layer ingestion failed: {e}")
        results = {
            "timestamp": get_current_timestamp(),
            "overall_status": "failed",
            "error": str(e)
        }
        raise

# COMMAND ----------

# Execute main function
import pyspark.sql.functions as F

results = main()

# Exit with results for downstream notebooks
dbutils.notebook.exit(results) 