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

df = spark.read.option("multiline", "true").option("header", "true").csv(f"{bronze_adls}/data.csv")


# COMMAND ----------

from datetime import datetime
now = datetime.now().strftime("%Y-%m-%d")

df.write.format('delta') \
    .mode("overwrite") \
    .save(f"{silver_adls}/data{now}")

# COMMAND ----------

output_data = {
    "now": now,
    "saved": f"{silver_adls}/data{now}",
    "bronze_adls": bronze_adls,
    "silver_adls": silver_adls,
    "gold_adls": gold_adls
}
dbutils.notebook.exit(output_data)