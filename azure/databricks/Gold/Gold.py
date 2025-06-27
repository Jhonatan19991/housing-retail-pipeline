# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Analytics & Data Processing
# MAGIC 
# MAGIC This notebook handles the final data preparation and analytics in the Gold layer.
# MAGIC 
# MAGIC ## Features:
# MAGIC - Advanced data preparation
# MAGIC - Feature engineering for analytics
# MAGIC - Data quality validation
# MAGIC - Performance optimizations
# MAGIC - Analytics-ready dataset creation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration and Setup

# COMMAND ----------

import logging
from datetime import datetime
from typing import Dict, Any, Optional
import json
import numpy as np
import pandas as pd
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
    "processing": {
        "batch_size": 10000,
        "optimize_write": True,
        "partition_columns": ["department", "city"]
    },
    "feature_engineering": {
        "price_per_sqm": True,
        "area_ratio": True,
        "poi_density": True,
        "categorical_encoding": "onehot"
    },
    "data_quality": {
        "min_price": 1000000,  # 1M COP
        "max_price": 5000000000,  # 5B COP
        "min_area": 20,  # 20 m²
        "max_area": 1000,  # 1000 m²
        "required_columns": ["department", "city", "precio", "área_construida"]
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
        "outliers": {},
        "data_distribution": {}
    }
    
    # Check null counts
    for col in df.columns:
        null_count = df.filter(f"{col} IS NULL").count()
        quality_report["null_counts"][col] = null_count
    
    # Check for price outliers
    if "Precio" in df.columns:
        try:
            price_stats = df.select(
                min("Precio").alias("min_price"),
                max("Precio").alias("max_price"),
                avg("Precio").alias("avg_price"),
                stddev("Precio").alias("std_price")
            ).collect()[0]
            
            quality_report["data_distribution"]["price"] = {
                "min": price_stats["min_price"],
                "max": price_stats["max_price"],
                "avg": price_stats["avg_price"],
                "std": price_stats["std_price"]
            }
            
            # Count outliers
            min_price = CONFIG["data_quality"]["min_price"]
            max_price = CONFIG["data_quality"]["max_price"]
            outliers = df.filter(
                (col("Precio") < min_price) | (col("Precio") > max_price)
            ).count()
            quality_report["outliers"]["price"] = outliers
            
        except Exception as e:
            quality_report["outliers"]["price"] = f"Error: {str(e)}"
    
    # Check for area outliers
    for area_col in ["Área_Construida", "Área_Privada"]:
        if area_col in df.columns:
            try:
                min_area = CONFIG["data_quality"]["min_area"]
                max_area = CONFIG["data_quality"]["max_area"]
                
                outliers = df.filter(
                    (col(area_col) < min_area) | (col(area_col) > max_area)
                ).count()
                quality_report["outliers"][area_col] = outliers
            except Exception as e:
                quality_report["outliers"][area_col] = f"Error: {str(e)}"
    
    logger.info(f"🔍 Data Quality Report for {name}:")
    logger.info(f"   - Total rows: {quality_report['total_rows']:,}")
    logger.info(f"   - Duplicate rows: {quality_report['duplicate_rows']:,}")
    logger.info(f"   - Null counts: {quality_report['null_counts']}")
    logger.info(f"   - Outliers: {quality_report['outliers']}")
    
    return quality_report

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Preparation Functions

# COMMAND ----------

def prepare_final_dataset(df: DataFrame) -> DataFrame:
    """Prepare the final dataset for analytics."""
    logger.info("🔄 Preparing final dataset...")
    
    # Select relevant columns
    selected_columns = [
        'department', 'city', 'lat', 'lon', 'hospital', 'mall', 'park', 'school',
        'Estrato', 'Tipo_de_Inmueble', 'estado', 'Baños', 'Habitaciones', 
        'Área_Construida', 'Área_Privada', 'parqueaderos', 'Financiacion', 
        'Pisos_interiores', 'Precio'
    ]
    
    # Filter existing columns
    existing_columns = [col for col in selected_columns if col in df.columns]
    df = df.select(*existing_columns)
    
    # Remove duplicates
    df = df.dropDuplicates()
    
    # Remove null values
    df = df.dropna()
    
    # Apply data quality filters
    df = df.filter(
        (col("Precio") >= CONFIG["data_quality"]["min_price"]) &
        (col("Precio") <= CONFIG["data_quality"]["max_price"]) &
        (col("Área_Construida") >= CONFIG["data_quality"]["min_area"]) &
        (col("Área_Construida") <= CONFIG["data_quality"]["max_area"])
    )
    
    logger.info(f"✅ Final dataset prepared: {df.count()} rows, {len(df.columns)} columns")
    return df

def convert_to_pandas(df: DataFrame) -> pd.DataFrame:
    """Convert Spark DataFrame to Pandas with optimization."""
    logger.info("🔄 Converting to Pandas DataFrame...")
    
    # Optimize conversion by selecting only needed columns
    pandas_df = df.toPandas()
    
    logger.info(f"✅ Converted to Pandas: {pandas_df.shape}")
    return pandas_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Engineering Functions

# COMMAND ----------

def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """Engineer features for analytics."""
    logger.info("🔄 Engineering features...")
    
    # Create price per square meter
    if CONFIG["feature_engineering"]["price_per_sqm"]:
        df['precio_por_m2'] = df['Precio'] / df['Área_Construida']
        df['precio_por_m2'] = df['precio_por_m2'].replace([np.inf, -np.inf], np.nan)
        df = df.dropna(subset=['precio_por_m2'])
    
    # Create area ratio (private vs constructed)
    if CONFIG["feature_engineering"]["area_ratio"]:
        df['ratio_area'] = df['Área_Privada'] / df['Área_Construida']
        df['ratio_area'] = df['ratio_area'].replace([np.inf, -np.inf], 1.0)  # Default to 1.0
        df['ratio_area'] = df['ratio_area'].fillna(1.0)
    
    # Create POI density features
    if CONFIG["feature_engineering"]["poi_density"]:
        poi_columns = ['hospital', 'mall', 'park', 'school']
        existing_poi_cols = [col for col in poi_columns if col in df.columns]
        
        if existing_poi_cols:
            df['poi_total'] = df[existing_poi_cols].sum(axis=1)
            df['poi_density'] = df['poi_total'] / df['Área_Construida']
            df['poi_density'] = df['poi_density'].replace([np.inf, -np.inf], 0)
            df['poi_density'] = df['poi_density'].fillna(0)
    
    # Create interaction features
    df['estrato_area'] = df['Estrato'] * df['Área_Construida']
    df['habitaciones_baños'] = df['Habitaciones'] * df['Baños']
    
    # Create location-based features
    if 'lat' in df.columns and 'lon' in df.columns:
        df['lat_lon_combined'] = df['lat'] * df['lon']
    
    # Create additional analytics features
    df['precio_por_habitacion'] = df['Precio'] / df['Habitaciones']
    df['precio_por_habitacion'] = df['precio_por_habitacion'].replace([np.inf, -np.inf], np.nan)
    df['precio_por_habitacion'] = df['precio_por_habitacion'].fillna(df['Precio'])
    
    df['area_por_habitacion'] = df['Área_Construida'] / df['Habitaciones']
    df['area_por_habitacion'] = df['area_por_habitacion'].replace([np.inf, -np.inf], np.nan)
    df['area_por_habitacion'] = df['area_por_habitacion'].fillna(df['Área_Construida'])
    
    logger.info("✅ Features engineered successfully")
    return df

def encode_categorical_features(df: pd.DataFrame) -> pd.DataFrame:
    """Encode categorical features for analytics."""
    logger.info("🔄 Encoding categorical features...")
    
    categorical_columns = ['department', 'city', 'Tipo_de_Inmueble', 'estado']
    existing_cat_cols = [col for col in categorical_columns if col in df.columns]
    
    if CONFIG["feature_engineering"]["categorical_encoding"] == "onehot":
        # One-hot encoding
        df_encoded = pd.get_dummies(df, columns=existing_cat_cols, drop_first=True)
    else:
        # Label encoding
        df_encoded = df.copy()
        from sklearn.preprocessing import LabelEncoder
        
        for col in existing_cat_cols:
            le = LabelEncoder()
            df_encoded[col] = le.fit_transform(df_encoded[col].astype(str))
    
    logger.info(f"✅ Categorical features encoded: {len(existing_cat_cols)} columns")
    return df_encoded

def remove_outliers(df: pd.DataFrame, target_col: str = 'Precio') -> pd.DataFrame:
    """Remove outliers based on percentiles."""
    logger.info("🔄 Removing outliers...")
    
    initial_rows = len(df)
    
    # Remove outliers from target variable using IQR method
    Q1 = df[target_col].quantile(0.25)
    Q3 = df[target_col].quantile(0.75)
    IQR = Q3 - Q1
    
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    
    df_clean = df[
        (df[target_col] >= lower_bound) & 
        (df[target_col] <= upper_bound)
    ]
    
    removed_rows = initial_rows - len(df_clean)
    logger.info(f"✅ Outliers removed: {removed_rows} rows ({removed_rows/initial_rows*100:.1f}%)")
    
    return df_clean

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analytics Functions

# COMMAND ----------

def generate_analytics_summary(df: pd.DataFrame) -> Dict[str, Any]:
    """Generate comprehensive analytics summary."""
    logger.info("🔄 Generating analytics summary...")
    
    summary = {
        "timestamp": get_current_timestamp(),
        "dataset_info": {
            "total_records": len(df),
            "total_columns": len(df.columns),
            "memory_usage_mb": df.memory_usage(deep=True).sum() / 1024 / 1024
        },
        "price_analytics": {},
        "area_analytics": {},
        "location_analytics": {},
        "categorical_analytics": {}
    }
    
    # Price analytics
    if 'Precio' in df.columns:
        summary["price_analytics"] = {
            "mean": df['Precio'].mean(),
            "median": df['Precio'].median(),
            "std": df['Precio'].std(),
            "min": df['Precio'].min(),
            "max": df['Precio'].max(),
            "q25": df['Precio'].quantile(0.25),
            "q75": df['Precio'].quantile(0.75)
        }
    
    # Area analytics
    if 'Área_Construida' in df.columns:
        summary["area_analytics"] = {
            "mean": df['Área_Construida'].mean(),
            "median": df['Área_Construida'].median(),
            "std": df['Área_Construida'].std(),
            "min": df['Área_Construida'].min(),
            "max": df['Área_Construida'].max()
        }
    
    # Location analytics
    if 'department' in df.columns:
        summary["location_analytics"]["departments"] = df['department'].value_counts().to_dict()
    if 'city' in df.columns:
        summary["location_analytics"]["cities"] = df['city'].value_counts().head(10).to_dict()
    
    # Categorical analytics
    categorical_cols = ['Estrato', 'Tipo_de_Inmueble', 'estado', 'Financiacion']
    for col in categorical_cols:
        if col in df.columns:
            summary["categorical_analytics"][col] = df[col].value_counts().to_dict()
    
    # Feature correlations
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    if len(numeric_cols) > 1:
        correlation_matrix = df[numeric_cols].corr()
        summary["correlations"] = {
            "price_correlations": correlation_matrix['Precio'].sort_values(ascending=False).to_dict()
        }
    
    logger.info("✅ Analytics summary generated successfully")
    return summary

def create_analytics_views(df: pd.DataFrame):
    """Create analytics views for business intelligence."""
    logger.info("🔄 Creating analytics views...")
    
    # Convert back to Spark DataFrame
    spark_df = spark.createDataFrame(df)
    
    # Create temporary views for analytics
    spark_df.createOrReplaceTempView("gold_analytics_data")
    
    # Create summary views
    summary_queries = {
        "price_by_department": """
            SELECT 
                department,
                COUNT(*) as total_properties,
                AVG(Precio) as avg_price,
                MEDIAN(Precio) as median_price,
                MIN(Precio) as min_price,
                MAX(Precio) as max_price
            FROM gold_analytics_data
            GROUP BY department
            ORDER BY avg_price DESC
        """,
        
        "price_by_type": """
            SELECT 
                Tipo_de_Inmueble,
                COUNT(*) as total_properties,
                AVG(Precio) as avg_price,
                AVG(precio_por_m2) as avg_price_per_sqm
            FROM gold_analytics_data
            GROUP BY Tipo_de_Inmueble
            ORDER BY avg_price DESC
        """,
        
        "area_analysis": """
            SELECT 
                department,
                AVG(Área_Construida) as avg_area,
                AVG(Área_Privada) as avg_private_area,
                AVG(ratio_area) as avg_area_ratio
            FROM gold_analytics_data
            GROUP BY department
            ORDER BY avg_area DESC
        """,
        
        "poi_analysis": """
            SELECT 
                department,
                AVG(poi_total) as avg_poi_count,
                AVG(poi_density) as avg_poi_density,
                AVG(hospital) as avg_hospitals,
                AVG(mall) as avg_malls,
                AVG(park) as avg_parks,
                AVG(school) as avg_schools
            FROM gold_analytics_data
            GROUP BY department
            ORDER BY avg_poi_density DESC
        """
    }
    
    # Execute and display summary views
    for view_name, query in summary_queries.items():
        try:
            result = spark.sql(query)
            logger.info(f"📊 {view_name} view created successfully")
            display(result)
        except Exception as e:
            logger.warning(f"⚠️ Could not create {view_name} view: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Export Functions

# COMMAND ----------

def save_processed_data(df: pd.DataFrame, quality_report: Dict[str, Any]):
    """Save processed data to Gold layer."""
    logger.info("🔄 Saving processed data to Gold layer...")
    
    # Convert back to Spark DataFrame
    spark_df = spark.createDataFrame(df)
    
    # Add processing metadata
    spark_df = spark_df.withColumn("processing_timestamp", current_timestamp()) \
                      .withColumn("batch_id", lit(get_current_timestamp()))
    
    # Write to Gold layer
    output_path = f'{gold_adls}/gold_data_{get_current_timestamp()}'
    spark_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(output_path)
    
    # Also save as latest version
    latest_path = f'{gold_adls}/gold_data'
    spark_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(latest_path)
    
    logger.info(f"✅ Data saved to: {output_path}")
    logger.info(f"✅ Latest version saved to: {latest_path}")
    
    return {
        "output_path": output_path,
        "latest_path": latest_path,
        "quality_report": quality_report,
        "timestamp": get_current_timestamp()
    }

def save_analytics_results(analytics_summary: Dict[str, Any]):
    """Save analytics results and summary."""
    logger.info("🔄 Saving analytics results...")
    
    # Save results to JSON
    results_path = f'{gold_adls}/analytics_summary_{get_current_timestamp()}.json'
    dbutils.fs.put(results_path, json.dumps(analytics_summary, indent=2, default=str), overwrite=True)
    
    logger.info(f"✅ Analytics results saved to: {results_path}")
    return analytics_summary

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Execution

# COMMAND ----------

def main():
    """Main execution function with comprehensive error handling."""
    try:
        logger.info("🚀 Starting Gold layer processing...")
        
        # Initialize results
        results = {
            "timestamp": get_current_timestamp(),
            "processing_status": "success",
            "outputs": {}
        }
        
        # Load and prepare data
        try:
            # Load data from Silver layer
            df = spark.read.format("delta").load(f'{silver_adls}/silver_clean_data')
            
            if not validate_dataframe(df, "Silver Data"):
                raise Exception("Silver data validation failed")
            
            log_dataframe_info(df, "Silver Data")
            
            # Prepare final dataset
            df = prepare_final_dataset(df)
            quality_report = check_data_quality(df, "Final Dataset")
            
            # Convert to Pandas for feature engineering
            pandas_df = convert_to_pandas(df)
            
            # Feature engineering
            pandas_df = engineer_features(pandas_df)
            pandas_df = encode_categorical_features(pandas_df)
            pandas_df = remove_outliers(pandas_df)
            
            # Generate analytics
            analytics_summary = generate_analytics_summary(pandas_df)
            create_analytics_views(pandas_df)
            
            # Save processed data
            save_results = save_processed_data(pandas_df, quality_report)
            results["outputs"]["data_processing"] = save_results
            
            # Save analytics results
            analytics_results = save_analytics_results(analytics_summary)
            results["outputs"]["analytics"] = analytics_results
            
        except Exception as e:
            logger.error(f"Gold layer processing failed: {e}")
            results["outputs"]["processing"] = {"status": "failed", "error": str(e)}
            results["processing_status"] = "failed"
        
        # Log final results
        logger.info(f"🏁 Gold layer processing completed with status: {results['processing_status']}")
        
        return results
        
    except Exception as e:
        logger.error(f"❌ Gold layer processing failed: {e}")
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

# Exit with results
dbutils.notebook.exit(results) 