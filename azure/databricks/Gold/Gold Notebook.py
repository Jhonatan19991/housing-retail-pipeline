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

df = spark.read.format("delta").load(f'{silver_adls}/silver_clean_data')

df = df.select('department', 'city', 'lat', 'lon', 'hospital', 'mall', 'park', 'school','Estrato','Tipo_de_Inmueble','estado', 'Baños', 'Habitaciones', 'Área_Construida', 'Área_Privada',  'parqueaderos', 'Financiacion', 'Pisos_interiores','Precio')
df = df.dropDuplicates()

df = df.dropna()


df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f'{gold_adls}/gold_data')

# COMMAND ----------

df = df.toPandas()

print(df)

# COMMAND ----------

df.head()

# COMMAND ----------

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, GridSearchCV, KFold
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import r2_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

# 1) Dummies (igual que antes)
cat_cols = ['department','city','Estrato','Tipo_de_Inmueble','estado','Financiacion']
df = pd.get_dummies(df, columns=cat_cols, drop_first=True)

X = df.drop('Precio',1)
y = df['Precio']

# 2) Filtrado de outliers (1º y 99º percentil)
low, high = y.quantile(0.01), y.quantile(0.99)
mask = y.between(low, high)
X, y = X[mask], y[mask]

# 3) train/test split
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

# 4) Pipeline: scaler (opcional) + RF
pipe = Pipeline([
    ('scaler', StandardScaler()),      # opcional
    ('rf', RandomForestRegressor(random_state=42))
])

param_grid = {
    'rf__n_estimators': [100, 300],
    'rf__max_depth': [None, 10, 30],
    'rf__min_samples_leaf': [1, 5],
    'rf__max_features': ['sqrt', 0.5]
}

cv = KFold(n_splits=5, shuffle=True, random_state=42)
grid = GridSearchCV(
    pipe,
    param_grid,
    cv=cv,
    scoring='r2',
    n_jobs=-1,
    verbose=1
)

# 5) entrenar sobre log(Precio)
y_train_log = np.log1p(y_train)
grid.fit(X_train, y_train_log)

# 6) predecir y evaluar
y_pred_log = grid.predict(X_test)
y_pred = np.expm1(y_pred_log)

print("Mejores hiperparámetros:", grid.best_params_)
print("R² en test:", r2_score(y_test, y_pred).round(4))
