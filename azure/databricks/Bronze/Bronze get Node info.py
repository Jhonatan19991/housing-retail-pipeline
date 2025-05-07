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



import requests

OVERPASS_URL = "https://overpass-api.de/api/interpreter"


overpass_query = """
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
out body;      // geometría y tags :contentReference[oaicite:1]{index=1}
>;             // expande ways a nodos :contentReference[oaicite:2]{index=2}
out skel qt;   // salida ligera con coordenadas :contentReference[oaicite:3]{index=3}

"""
# 2. Envía la petición POST a Overpass
response = requests.post(
    OVERPASS_URL,
    data={"data": overpass_query},
    timeout=600  # 10 minutos para descarga completa
)
response.raise_for_status()

# 3. Guarda el JSON resultante en DBFS
dbutils.fs.put(
    f"{bronze_adls}/pois_colombia.json",
    response.text,
    overwrite=True
)