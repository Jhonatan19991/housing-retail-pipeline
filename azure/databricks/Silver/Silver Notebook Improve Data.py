# Databricks notebook source

tiers = ["bronze", "silver", "gold"]
adls_paths = {
    tier: f"abfss://{tier}@jhonastorage23.dfs.core.windows.net/"
     for tier in tiers 
    }

bronze_adls = adls_paths["bronze"]
silver_adls = adls_paths["silver"]
gold_adls = adls_paths["gold"]

dbutils.fs.ls(bronze_adls)
dbutils.fs.ls(silver_adls)
dbutils.fs.ls(gold_adls)


# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType
from pyspark.sql.functions import explode, col, broadcast
import h3spark



df_housing = spark.read.format("delta").load(f'{silver_adls}/housing_silver_deduplicate')

# COMMAND ----------

# antes de leer
spark.conf.set("spark.sql.caseSensitive", True)

poi_df = spark.read \
    .option("multiline","true") \
    .json(f"{bronze_adls}/pois_colombia.json")


# COMMAND ----------



# 1) lectura multilínea: genera un DataFrame con una sola fila y una columna array llamada "pois"
raw_df = spark.read.option("multiLine","true") \
                  .json(f"{bronze_adls}/pois_colombia.json")

# 2) explode: una fila por cada elemento del array
exploded_df = raw_df.withColumn("poi", explode(col("elements")))   # “pois” es el nombre del array raíz

# 3) aplanar la estructura: cada campo de “poi” se convierte en columna
poi_df = exploded_df.select("poi.*")



# COMMAND ----------

RESOLUTION = 8

# 1) cast to double
clean_df = poi_df \
  .withColumn("lat", F.col("lat").cast("double")) \
  .withColumn("lon", F.col("lon").cast("double")) \
  .na.drop(subset=["lat","lon"])       # filter out any nulls

# 2) compute H3 index with correct param order
clean_df = clean_df.withColumn(
    "hex_id",
    h3spark.latlng_to_cell(F.col("lat"), F.col("lon"), F.lit(RESOLUTION))
)

# 3) continue with your aggregation
counts_df = (
  clean_df
  .withColumn(
    "category",
    F.coalesce(F.col("tags.amenity"), F.col("tags.shop"), F.col("tags.leisure"))
  )
  .groupBy("hex_id", "category")
  .count()
)

pivot_df = counts_df.groupBy("hex_id").pivot("category").sum("count")
df_housing = spark.read.format("delta").load(f'{silver_adls}/housing_silver_deduplicate')

df_housing = df_housing.withColumn('lat', F.regexp_extract(F.col("lat_lon"), r"\(([^,]+),", 1)) \
    .withColumn('lon', F.regexp_extract(F.col("lat_lon"), r",\s*([^)]+)\)", 1))

df_housing = df_housing \
  .withColumn('lat', F.col('lat').cast('double')) \
  .withColumn('lon', F.col('lon').cast('double'))


df_housing = df_housing.withColumn(
    'hex_id',
    h3spark.latlng_to_cell(F.col('lat'), F.col('lon'), F.lit(8))
)

result = df_housing.join(broadcast(pivot_df), "hex_id", "left")

result = result.select("id_house", "fecha", "url", "titulo", "constructor", "item", "unidades", "full_info", "direccion_principal", "direccion_asociadas", "lat_lon", "department", "city", "lat", "lon", "hospital", "mall", "park",  "school")


# COMMAND ----------

result.write.format("delta").mode("overwrite").save(f'{silver_adls}/silver_improve_data')