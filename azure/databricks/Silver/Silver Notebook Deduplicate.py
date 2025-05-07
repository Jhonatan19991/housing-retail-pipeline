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

dbutils.widgets.text("input2", "", "")
valor_recibido = dbutils.widgets.get("input2")
from datetime import datetime
now = datetime.now().strftime("%Y-%m-%d")


valor_recibido = f"{silver_adls}/data{now}"
print(valor_recibido)

# COMMAND ----------

from datetime import datetime
now = datetime.now().strftime("%Y-%m-%d")
ruta = f"{silver_adls}/data{now}"

# Carga y crea la vista en un solo paso
df = spark.read.format("delta").load(ruta)
df.createOrReplaceTempView("updating_housing")


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS housing_silver_deduplicate (
# MAGIC   id_house BIGINT GENERATED ALWAYS AS IDENTITY,           
# MAGIC   fecha DATE,
# MAGIC   url STRING,
# MAGIC   titulo STRING,
# MAGIC   constructor STRING,
# MAGIC   item VARIANT,                                      
# MAGIC   unidades ARRAY<VARIANT>,                           
# MAGIC   full_info VARIANT,                                 
# MAGIC   direccion_principal STRING,
# MAGIC   direccion_asociadas STRING,
# MAGIC   lat_lon STRING,
# MAGIC   department STRING,
# MAGIC   city STRING           
# MAGIC ) USING DELTA LOCATION 'abfss://silver@jhonastorage23.dfs.core.windows.net/housing_silver_deduplicate'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH deduplicated_updating_housing AS (
# MAGIC   SELECT 
# MAGIC     fecha,
# MAGIC     url,
# MAGIC     titulo,
# MAGIC     constructor,
# MAGIC     item,
# MAGIC     SPLIT(unidades, ',') AS unidades, 
# MAGIC     full_info,
# MAGIC     direccion_principal,
# MAGIC     direccion_asociadas,
# MAGIC     lat_lon AS lat_lon, 
# MAGIC     department,
# MAGIC     city,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY url ORDER BY fecha DESC) AS row_num
# MAGIC   FROM updating_housing
# MAGIC )
# MAGIC MERGE INTO housing_silver_deduplicate t
# MAGIC USING (
# MAGIC   SELECT 
# MAGIC     fecha,
# MAGIC     url,
# MAGIC     titulo,
# MAGIC     constructor,
# MAGIC     item,
# MAGIC     unidades, 
# MAGIC     full_info,
# MAGIC     direccion_principal,
# MAGIC     direccion_asociadas,
# MAGIC     lat_lon, 
# MAGIC     department,
# MAGIC     city
# MAGIC   FROM deduplicated_updating_housing
# MAGIC   WHERE row_num = 1
# MAGIC ) u
# MAGIC ON t.url = u.url
# MAGIC WHEN MATCHED THEN 
# MAGIC   UPDATE SET 
# MAGIC     t.fecha = u.fecha,
# MAGIC     t.titulo = u.titulo,
# MAGIC     t.constructor = u.constructor,
# MAGIC     t.item = u.item,
# MAGIC     t.unidades = u.unidades,
# MAGIC     t.full_info = u.full_info,
# MAGIC     t.direccion_principal = u.direccion_principal,
# MAGIC     t.direccion_asociadas = u.direccion_asociadas,
# MAGIC     t.lat_lon = u.lat_lon,
# MAGIC     t.department = u.department,
# MAGIC     t.city = u.city
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC   INSERT (fecha, url, titulo, constructor, item, unidades, full_info, direccion_principal, direccion_asociadas, lat_lon, department, city)
# MAGIC   VALUES (u.fecha, u.url, u.titulo, u.constructor, u.item, u.unidades, u.full_info, u.direccion_principal, u.direccion_asociadas, u.lat_lon, u.department, u.city)