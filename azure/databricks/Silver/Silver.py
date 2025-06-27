# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Data Processing & Transformation
# MAGIC 
# MAGIC This notebook handles data processing, cleaning, and transformation in the Silver layer.
# MAGIC 
# MAGIC ## Features:
# MAGIC - Schema management and validation
# MAGIC - Data quality checks and monitoring
# MAGIC - Performance optimizations
# MAGIC - Modular transformation functions
# MAGIC - Error handling and logging
# MAGIC - Data lineage tracking

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration and Setup

# COMMAND ----------

import logging
from datetime import datetime
from typing import Dict, Any, Optional, List
import json
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import DataFrame
import h3spark

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
CONFIG = {
    "storage": {
        "account": "jhonastorage23",
        "tiers": ["bronze", "silver", "gold"]
    },
    "processing": {
        "h3_resolution": 8,
        "batch_size": 10000,
        "optimize_write": True,
        "partition_columns": ["department", "city"]
    },
    "data_quality": {
        "min_price": 1000000,  # 1M COP
        "max_price": 5000000000,  # 5B COP
        "min_area": 20,  # 20 m²
        "max_area": 1000,  # 1000 m²
        "required_columns": ["url", "titulo", "precio"]
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
# MAGIC ## Schema Definitions

# COMMAND ----------

# Define schemas for better performance and data validation
SCHEMAS = {
    "item": StructType([
        StructField("Estado", StringType(), True),
        StructField("Estrato", StringType(), True),
        StructField("Parqueaderos", StringType(), True),
        StructField("Financiación", StringType(), True),
        StructField("Cuota inicial", StringType(), True),
        StructField("Pisos interiores", StringType(), True),
        StructField("Aplica subsidio", StringType(), True)
    ]),
    
    "units": ArrayType(StructType([
        StructField("m² const.", StringType(), True),
        StructField("m² priv.", StringType(), True),
        StructField("Tipo de Inmueble", StringType(), True),
        StructField("Hab/Amb", StringType(), True),
        StructField("Baños", StringType(), True),
        StructField("Precio", StringType(), True)
    ])),
    
    "full_info": StructType([
        StructField("Estrato", StringType(), True),
        StructField("Tipo de Inmueble", StringType(), True),
        StructField("Estado", StringType(), True),
        StructField("Baños", StringType(), True),
        StructField("Área Construida", StringType(), True),
        StructField("Área Privada", StringType(), True),
        StructField("Antigüedad", StringType(), True),
        StructField("Habitaciones", StringType(), True),
        StructField("Parqueaderos", StringType(), True),
        StructField("Acepta permuta", StringType(), True),
        StructField("Remodelado", StringType(), True),
        StructField("Precio", StringType(), True)
    ])
}

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
    required_cols = CONFIG["data_quality"]["required_columns"]
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

def check_data_quality(df: DataFrame, name: str) -> Dict[str, Any]:
    """Perform comprehensive data quality checks."""
    quality_report = {
        "total_rows": df.count(),
        "null_counts": {},
        "duplicate_rows": df.count() - df.dropDuplicates().count(),
        "data_type_issues": {},
        "outliers": {}
    }
    
    # Check null counts
    for col in df.columns:
        null_count = df.filter(f"{col} IS NULL").count()
        quality_report["null_counts"][col] = null_count
    
    # Check for price outliers if price column exists
    if "Precio" in df.columns:
        try:
            price_col = col("Precio").cast("double")
            min_price = CONFIG["data_quality"]["min_price"]
            max_price = CONFIG["data_quality"]["max_price"]
            
            outliers = df.filter(
                (price_col < min_price) | (price_col > max_price)
            ).count()
            quality_report["outliers"]["price"] = outliers
        except:
            quality_report["outliers"]["price"] = "Error checking price outliers"
    
    # Check for area outliers
    for area_col in ["Área_Construida", "Área_Privada"]:
        if area_col in df.columns:
            try:
                area_col_expr = col(area_col).cast("double")
                min_area = CONFIG["data_quality"]["min_area"]
                max_area = CONFIG["data_quality"]["max_area"]
                
                outliers = df.filter(
                    (area_col_expr < min_area) | (area_col_expr > max_area)
                ).count()
                quality_report["outliers"][area_col] = outliers
            except:
                quality_report["outliers"][area_col] = "Error checking area outliers"
    
    logger.info(f"🔍 Data Quality Report for {name}:")
    logger.info(f"   - Total rows: {quality_report['total_rows']:,}")
    logger.info(f"   - Duplicate rows: {quality_report['duplicate_rows']:,}")
    logger.info(f"   - Null counts: {quality_report['null_counts']}")
    logger.info(f"   - Outliers: {quality_report['outliers']}")
    
    return quality_report

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Transformation Functions

# COMMAND ----------

def parse_json_columns(df: DataFrame) -> DataFrame:
    """Parse JSON columns with proper error handling."""
    logger.info("🔄 Parsing JSON columns...")
    
    # Parse full_info column
    if "full_info" in df.columns:
        df = df.withColumn('full_info', from_json(col('full_info').cast('string'), SCHEMAS["full_info"]))
        
        # Extract fields from full_info
        full_info_fields = [
            "Estrato", "Tipo de Inmueble", "Estado", "Baños", 
            "Área Construida", "Área Privada", "Antigüedad", 
            "Habitaciones", "Parqueaderos", "Acepta permuta", 
            "Remodelado", "Precio"
        ]
        
        for field in full_info_fields:
            df = df.withColumn(field, col(f"full_info.{field}"))
        
        df = df.drop('full_info')
    
    # Parse item column
    if "item" in df.columns:
        df = df.withColumn("item", from_json(col("item").cast("string"), SCHEMAS["item"]))
        
        # Use coalesce to prioritize full_info values over item values
        df = df.withColumn("Estrato", coalesce(col("Estrato"), col("item.Estrato"))) \
               .withColumn("Estado", coalesce(col("Estado"), col("item.Estado"))) \
               .withColumn("Parqueaderos", coalesce(col("Parqueaderos"), col("item.Parqueaderos"))) \
               .withColumn("Financiacion", col("item.Financiación")) \
               .withColumn("Cuota_inicial", col("item.`Cuota inicial`")) \
               .withColumn("Pisos_interiores", col("item.`Pisos interiores`")) \
               .withColumn("Aplica_subsidio", col("item.`Aplica subsidio`"))
        
        df = df.drop("item")
    
    logger.info("✅ JSON columns parsed successfully")
    return df

def parse_units_column(df: DataFrame) -> DataFrame:
    """Parse and explode units column with proper JSON handling."""
    logger.info("🔄 Parsing units column...")
    
    if "unidades" not in df.columns:
        logger.warning("⚠️ Units column not found, skipping units parsing")
        return df
    
    # Clean and parse JSON string
    df = df.withColumn("json_str", concat_ws("", col("unidades"))) \
           .withColumn("json_str", regexp_replace(col("json_str"), "'", "\"")) \
           .withColumn("json_str", regexp_replace(col("json_str"), r"\}\s*\{", "},{")) \
           .withColumn("json_str", regexp_replace(col("json_str"), r'"\s+"', '", "')) \
           .withColumn("unidades", from_json(col("json_str"), SCHEMAS["units"]))
    
    # Explode units and coalesce with existing columns
    df = df.withColumn("unidades", explode_outer(col("unidades"))) \
           .withColumn("Área_Construida", coalesce(col('Área_Construida'), col("unidades.`m² const.`"))) \
           .withColumn("Área_Privada", coalesce(col('Área_Privada'), col("unidades.`m² priv.`"))) \
           .withColumn("Tipo_de_Inmueble", coalesce(col('Tipo_de_Inmueble'), col("unidades.`Tipo de Inmueble`"))) \
           .withColumn("Habitaciones", coalesce(col('Habitaciones'), col("unidades.`Hab/Amb`"))) \
           .withColumn("Baños", coalesce(col('Baños'), col("unidades.`Baños`"))) \
           .withColumn("Precio", coalesce(col("Precio"), col("unidades.`Precio`"))) \
           .drop("unidades", "json_str")
    
    logger.info("✅ Units column parsed successfully")
    return df

def clean_and_cast_columns(df: DataFrame) -> DataFrame:
    """Clean and cast columns to appropriate data types."""
    logger.info("🔄 Cleaning and casting columns...")
    
    # Clean price column
    df = df.withColumn("Precio", regexp_replace(col("Precio"), "[^0-9]", "").cast("double"))
    
    # Convert boolean-like columns
    df = df.withColumn("Financiacion", 
                      when((col("Financiacion").isNull()) | (col("Financiacion") == "No disponible"), 0).otherwise(1)) \
           .withColumn("Parqueaderos", 
                      when((col("Parqueaderos").isNull()) | (col("Parqueaderos") == "No disponible"), 0).otherwise(1))
    
    # Clean and cast habitaciones
    df = df.withColumn("Habitaciones", regexp_extract(col("Habitaciones"), r"(\d+)", 1).cast("int")) \
           .withColumn("Habitaciones", coalesce(col('Habitaciones'), lit(0)))
    
    # Set default values for POI columns
    df = df.withColumn('hospital', coalesce(col('hospital'), lit(0))) \
           .withColumn('mall', coalesce(col('mall'), lit(0))) \
           .withColumn('park', coalesce(col('park'), lit(0))) \
           .withColumn('school', coalesce(col('school'), lit(0)))
    
    # Cast numeric columns
    df = df.withColumn('Estrato', col('Estrato').cast('int')) \
           .withColumn('Baños', col('Baños').cast('int'))
    
    # Clean area columns
    for area_col in ["Área_Construida", "Área_Privada"]:
        if area_col in df.columns:
            df = df.withColumn(area_col, regexp_replace(col(area_col), r"(?i)\s*m2\s*$", "")) \
                   .withColumn(area_col, regexp_replace(col(area_col), r"[^0-9\.]", "")) \
                   .withColumn(area_col, col(area_col).cast("double"))
    
    # Handle empty area values
    df = df.withColumn("Área_Privada", 
                      when((col("Área_Privada") == "") | (col("Área_Privada").isNull()), 
                           col("Área_Construida")).otherwise(col("Área_Privada")))
    
    # Clean Pisos_interiores
    df = df.withColumn("Pisos_interiores", 
                      when((col("Pisos_interiores").isNull()) | (col("Pisos_interiores") == "No disponible"), 
                           1).otherwise(col("Pisos_interiores")))
    
    logger.info("✅ Columns cleaned and cast successfully")
    return df

def standardize_column_names(df: DataFrame) -> DataFrame:
    """Standardize column names by replacing spaces with underscores."""
    logger.info("🔄 Standardizing column names...")
    
    new_columns = [col.replace(" ", "_") for col in df.columns]
    df = df.toDF(*new_columns)
    
    logger.info("✅ Column names standardized")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## POI Data Processing

# COMMAND ----------

def process_poi_data() -> DataFrame:
    """Process POI data from Overpass API."""
    logger.info("🔄 Processing POI data...")
    
    try:
        # Read POI data
        poi_df = spark.read.option("multiline", "true").json(f"{bronze_adls}/pois_colombia.json")
        
        # Explode elements
        poi_df = poi_df.withColumn("poi", explode(col("elements"))).select("poi.*")
        
        # Clean and process coordinates
        poi_df = poi_df.withColumn("lat", col("lat").cast("double")) \
                      .withColumn("lon", col("lon").cast("double")) \
                      .na.drop(subset=["lat", "lon"])
        
        # Compute H3 index
        poi_df = poi_df.withColumn(
            "hex_id",
            h3spark.latlng_to_cell(col("lat"), col("lon"), lit(CONFIG["processing"]["h3_resolution"]))
        )
        
        # Categorize POIs
        poi_df = poi_df.withColumn(
            "category",
            coalesce(col("tags.amenity"), col("tags.shop"), col("tags.leisure"))
        )
        
        # Aggregate by hex_id and category
        counts_df = poi_df.groupBy("hex_id", "category").count()
        pivot_df = counts_df.groupBy("hex_id").pivot("category").sum("count").na.fill(0)
        
        logger.info("✅ POI data processed successfully")
        return pivot_df
        
    except Exception as e:
        logger.error(f"❌ POI data processing failed: {e}")
        # Return empty DataFrame with expected schema
        return spark.createDataFrame([], StructType([
            StructField("hex_id", StringType(), True),
            StructField("hospital", LongType(), True),
            StructField("mall", LongType(), True),
            StructField("park", LongType(), True),
            StructField("school", LongType(), True)
        ]))

def enrich_with_poi_data(df: DataFrame, poi_df: DataFrame) -> DataFrame:
    """Enrich housing data with POI information."""
    logger.info("🔄 Enriching data with POI information...")
    
    # Extract coordinates from lat_lon
    df = df.withColumn('lat', regexp_extract(col("lat_lon"), r"\(([^,]+),", 1)) \
           .withColumn('lon', regexp_extract(col("lat_lon"), r",\s*([^)]+)\)", 1)) \
           .withColumn('lat', col('lat').cast('double')) \
           .withColumn('lon', col('lon').cast('double'))
    
    # Compute H3 index for housing data
    df = df.withColumn(
        'hex_id',
        h3spark.latlng_to_cell(col('lat'), col('lon'), lit(CONFIG["processing"]["h3_resolution"]))
    )
    
    # Join with POI data using broadcast join for better performance
    result = df.join(broadcast(poi_df), "hex_id", "left")
    
    # Select final columns
    final_columns = [
        "id_house", "fecha", "url", "titulo", "constructor", "item", "unidades", 
        "full_info", "direccion_principal", "direccion_asociadas", "lat_lon", 
        "department", "city", "lat", "lon", "hospital", "mall", "park", "school"
    ]
    
    # Filter columns that exist in the DataFrame
    existing_columns = [col for col in final_columns if col in result.columns]
    result = result.select(*existing_columns)
    
    logger.info("✅ Data enriched with POI information")
    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Processing Functions

# COMMAND ----------

def process_housing_data() -> DataFrame:
    """Main function to process housing data through all transformations."""
    logger.info("🚀 Starting housing data processing...")
    
    # Read data from deduplication table
    df = spark.read.format("delta").load(f'{silver_adls}/housing_silver_deduplicate')
    
    if not validate_dataframe(df, "Raw Housing Data"):
        raise Exception("Raw housing data validation failed")
    
    log_dataframe_info(df, "Raw Housing Data")
    
    # Apply transformations
    df = parse_json_columns(df)
    df = parse_units_column(df)
    df = clean_and_cast_columns(df)
    df = standardize_column_names(df)
    
    # Process POI data and enrich
    poi_df = process_poi_data()
    df = enrich_with_poi_data(df, poi_df)
    
    # Final data quality check
    quality_report = check_data_quality(df, "Processed Housing Data")
    
    # Optimize DataFrame
    df = optimize_dataframe(df)
    
    logger.info("✅ Housing data processing completed")
    return df, quality_report

def save_processed_data(df: DataFrame, quality_report: Dict[str, Any]):
    """Save processed data with optimizations."""
    logger.info("🔄 Saving processed data...")
    
    # Add processing metadata
    df = df.withColumn("processing_timestamp", current_timestamp()) \
           .withColumn("batch_id", lit(get_current_timestamp()))
    
    # Write with optimizations
    write_options = {
        "format": "delta",
        "mode": "overwrite",
        "mergeSchema": "true"
    }
    
    if CONFIG["processing"]["optimize_write"]:
        write_options["optimizeWrite"] = "true"
    
    output_path = f'{silver_adls}/silver_clean_data_{get_current_timestamp()}'
    df.write.options(**write_options).save(output_path)
    
    # Also save as latest version
    latest_path = f'{silver_adls}/silver_clean_data'
    df.write.options(**write_options).save(latest_path)
    
    logger.info(f"✅ Data saved to: {output_path}")
    logger.info(f"✅ Latest version saved to: {latest_path}")
    
    return {
        "output_path": output_path,
        "latest_path": latest_path,
        "quality_report": quality_report,
        "timestamp": get_current_timestamp()
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Execution

# COMMAND ----------

def main():
    """Main execution function with comprehensive error handling."""
    try:
        logger.info("🚀 Starting Silver layer processing...")
        
        # Initialize results
        results = {
            "timestamp": get_current_timestamp(),
            "processing_status": "success",
            "outputs": {}
        }
        
        # Process housing data
        try:
            df, quality_report = process_housing_data()
            save_results = save_processed_data(df, quality_report)
            results["outputs"]["housing_processing"] = save_results
            
        except Exception as e:
            logger.error(f"Housing data processing failed: {e}")
            results["outputs"]["housing_processing"] = {"status": "failed", "error": str(e)}
            results["processing_status"] = "failed"
        
        # Log final results
        logger.info(f"🏁 Silver layer processing completed with status: {results['processing_status']}")
        
        return results
        
    except Exception as e:
        logger.error(f"❌ Silver layer processing failed: {e}")
        results = {
            "timestamp": get_current_timestamp(),
            "processing_status": "failed",
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