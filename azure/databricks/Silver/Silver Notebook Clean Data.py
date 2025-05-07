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

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType
import pyspark.sql.functions as F

schema_item = StructType([
    StructField("Estado",StringType(), True),
    StructField("Estrato", StringType(), True),
    StructField("Parqueaderos", StringType(), True),
    StructField("Financiación", StringType(), True),
    StructField("Cuota inicial", StringType(), True),
    StructField("Pisos interiores", StringType(), True),
    StructField("Aplica subsidio", StringType(), True) 
])

schema_units = ArrayType(StructType([
  StructField("m² const.", StringType(), True),
  StructField("m² priv.",  StringType(), True),
  StructField("Tipo de Inmueble", StringType(), True),
  StructField("Hab/Amb", StringType(), True),
  StructField("Baños", StringType(), True),
  StructField("Precio", StringType(), True)
]))


schema_full_info = StructType([
    StructField("Estrato",StringType(), True),
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


df_housing = spark.read.format("delta").load(f'{silver_adls}/silver_improve_data')

# COMMAND ----------

df_housing = df_housing.withColumn('full_info', F.from_json(F.col('full_info').cast('string'), schema_full_info)) \
    .withColumn('Estrato', F.col('full_info.Estrato')) \
    .withColumn('Tipo de Inmueble', F.col('full_info.`Tipo de Inmueble`')) \
        .withColumn('Estado', F.col('full_info.Estado')) \
        .withColumn('Baños', F.col('full_info.Baños')) \
        .withColumn('Área Construida', F.col('full_info.`Área Construida`')) \
        .withColumn('Área Privada', F.col('full_info.`Área Privada`')) \
        .withColumn('Antigüedad', F.col('full_info.Antigüedad')) \
        .withColumn('Habitaciones', F.col('full_info.Habitaciones')) \
        .withColumn('Parqueaderos', F.col('full_info.Parqueaderos')) \
        .withColumn('Acepta permuta', F.col('full_info.Acepta permuta')) \
        .withColumn('Remodelado', F.col('full_info.Remodelado')) \
        .withColumn('Precio', F.col('full_info.Precio')) \
    .drop('full_info')


# COMMAND ----------

df_housing = (
  df_housing
    .withColumn("item", F.from_json(F.col("item").cast("string"), schema_item))
    .withColumn("Estrato",    F.coalesce(F.col("Estrato"),    F.col("item.Estrato"))   )
    .withColumn("Estado",     F.coalesce(F.col("Estado"),     F.col("item.Estado"))    )
    .withColumn("Parqueaderos",F.coalesce(F.col("Parqueaderos"),F.col("item.Parqueaderos")))
    .withColumn("Financiacion", F.col("item.Financiación"))
    .withColumn("Cuota inicial", F.col("item.`Cuota inicial`") )
    .withColumn("Pisos interiores", F.col("item.`Pisos interiores`") )
    .withColumn("Aplica subsidio", F.col("item.`Aplica subsidio`") )
    .drop("item")
)

df_housing = df_housing.drop('item')


# COMMAND ----------

df_housing = df_housing.withColumn(
  "json_str",
  F.concat_ws("", F.col("unidades"))
)

df_housing = df_housing.withColumn(
  "json_str",
  F.regexp_replace(F.col("json_str"), "'", "\"")
)

df_housing = df_housing.withColumn(
  "json_str",
  F.regexp_replace(F.col("json_str"), r"\}\s*\{", "},{")
)

df_housing = df_housing.withColumn(
  "json_str",
  F.regexp_replace(F.col("json_str"), r'"\s+"', '", "')
)

df_housing = df_housing.withColumn(
  "unidades",
  F.from_json(F.col("json_str"), schema_units)
)

df_housing = df_housing.withColumn("unidades", F.explode_outer(F.col("unidades"))) \
    .withColumn("Área Construida", F.coalesce( F.col('Área Construida'), F.col("unidades.`m² const.`")) )\
    .withColumn("Área Privada", F.coalesce( F.col('Área Privada'), F.col("unidades.`m² priv.`"))) \
    .withColumn("Tipo de Inmueble", F.coalesce( F.col('Tipo de Inmueble'), F.col("unidades.`Tipo de Inmueble`"))) \
    .withColumn("Habitaciones", F.coalesce( F.col('Habitaciones'), F.col("unidades.`Hab/Amb`"))) \
    .withColumn("Baños", F.coalesce( F.col('Baños'), F.col("unidades.`Baños`"))) \
    .withColumn("Precio", F.coalesce(F.col("Precio"), F.col("unidades.`Precio`") )) \
    .drop("unidades", "json_str")


# COMMAND ----------

df_housing = df_housing.withColumn(
    "Precio",
    F.regexp_replace(F.col("Precio"), "[^0-9]", "").cast(DoubleType())
)

df_housing = df_housing.withColumn("Financiacion", F.when((F.col("Financiacion").isNull()) | (F.col("Financiacion") == "No disponible"), 0).otherwise(1))

df_housing = df_housing.withColumn("Parqueaderos", F.when((F.col("Parqueaderos").isNull()) | (F.col("Parqueaderos") == "No disponible"), 0).otherwise(1))

df_housing = df_housing.withColumn(
    "Habitaciones", 
    F.regexp_extract(F.col("Habitaciones"), r"(\d+)", 1).cast(IntegerType())

) \
    .withColumn(
    "Habitaciones", 
    F.coalesce(F.col('Habitaciones'), F.lit(0))
)

df_housing = df_housing.withColumn('hospital', F.coalesce(F.col('hospital'), F.lit(0))) \
    .withColumn('mall', F.coalesce(F.col('mall'), F.lit(0))) \
    .withColumn('park', F.coalesce(F.col('park'), F.lit(0))) \
    .withColumn('school', F.coalesce(F.col('school'), F.lit(0)))


df_housing = df_housing.withColumn('Estrato', F.col('Estrato').cast('int')) \
    .withColumn('Baños', F.col('Baños').cast('int')) 

# quit m2
df_housing = df_housing.withColumn(
    "Área Construida",
    F.regexp_replace(
        F.col("Área Construida"),
        r"(?i)\s*m2\s*$",   
        ""
    )
).withColumn(
    "Área Privada",
    F.regexp_replace(
        F.col("Área Privada"),
        r"(?i)\s*m2\s*$", 
        ""
    )
)

# remove pointers

df_housing = df_housing.withColumn(
    "Área Construida",
    F.regexp_replace(F.col("Área Construida"), r"[^0-9\.]", "")  
).withColumn(
    "Área Privada",
    F.regexp_replace(F.col("Área Privada"),  r"[^0-9\.]", "")
)

# CAST TO DOUBLE


df_housing = df_housing.withColumn(
    "Área Construida",
    F.col("Área Construida").cast(DoubleType())               # :contentReference[oaicite:5]{index=5}
).withColumn(
    "Área Privada",
    F.when(
        (F.col("Área Privada") == "") | (F.col("Área Privada").isNull()) ,                        # si quedó vacía
        F.col("Área Construida").cast(DoubleType())          # uso área construida
    ).otherwise(
        F.col("Área Privada").cast(DoubleType())
    )
)

# Reemplaza espacios por guiones bajos en todos los nombres de columnas
df_housing = df_housing.toDF(*[col.replace(" ", "_") for col in df_housing.columns])


df_housing = df_housing.withColumn("Pisos_interiores", F.when((F.col("Pisos_interiores").isNull()) | (F.col("Pisos_interiores") == "No disponible"), 1).otherwise(F.col("Pisos_interiores")) )

# COMMAND ----------

df_housing.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f'{silver_adls}/silver_clean_data')

# COMMAND ----------

display(df_housing)