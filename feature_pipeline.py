# -*- coding: utf-8 -*-
"""
Created on Mon Aug 18 12:17:47 2025

@author: Andres
"""

# feature_pipeline.py
# --------------------
# Este script de PySpark procesa datos de impresiones, clics y pagos para
# construir un dataset de features para un modelo de Machine Learning.
#
# Cómo ejecutarlo en una terminal (con Spark instalado):
# spark-submit feature_pipeline.py
# --------------------

import os
from datetime import timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, count, sum, max, date_sub

def create_spark_session():
    """Crea y retorna una sesión de Spark."""
    return SparkSession.builder \
        .appName("MercadoPago_Feature_Pipeline") \
        .master("local[*]")  # Usar "local" para pruebas, en un clúster real esto se gestiona diferente
        .getOrCreate()

def ingest_data(spark, data_paths):
    """Ingiere los tres data sources y retorna los DataFrames preparados."""
    
    # Cargar y aplanar Prints
    prints_df = spark.read.json(data_paths["prints"]) \
        .select(
            col("day").cast("date"),
            col("user_id").cast("long"),
            col("event_data.position").cast("integer").alias("position"),
            col("event_data.value_prop").alias("value_prop")
        )

    # Cargar y aplanar Taps
    taps_df = spark.read.json(data_paths["taps"]) \
        .select(
            col("day").cast("date"),
            col("user_id").cast("long"),
            col("event_data.position").cast("integer").alias("position"),
            col("event_data.value_prop").alias("value_prop")
        )

    # Cargar Payments
    pays_df = spark.read.csv(data_paths["pays"], header=True, inferSchema=True) \
        .select(
            col("pay_date").cast("date"),
            col("total").cast("double"),
            col("user_id").cast("long"),
            col("value_prop")
        )
        
    return {"prints": prints_df, "taps": taps_df, "pays": pays_df}

def engineer_features(dataframes):
    """Aplica la lógica de transformación y creación de features."""
    
    prints_df = dataframes["prints"]
    taps_df = dataframes["taps"]
    pays_df = dataframes["pays"]

    # 1. Definir ventanas de tiempo
    max_date_row = prints_df.agg(max("day")).first()
    if not max_date_row or not max_date_row[0]:
        raise ValueError("No se pudo determinar la fecha máxima en el archivo de prints.")
    
    max_date = max_date_row[0]
    last_week_start_date = max_date - timedelta(days=6)
    
    # 2. DataFrame base: prints de la última semana
    base_df = prints_df.filter(col("day") >= last_week_start_date)

    # 3. Feature 'was_clicked'
    taps_with_flag = taps_df.withColumn("clicked", lit(1))
    join_cols = ["day", "user_id", "position", "value_prop"]
    base_df = base_df.join(taps_with_flag, join_cols, "left") \
        .withColumn("was_clicked", when(col("clicked").isNotNull(), 1).otherwise(0)) \
        .drop("clicked")

    # 4. Agregaciones históricas (para todas las VPs vistas por el usuario)
    historical_prints = prints_df.filter(col("day") < last_week_start_date)
    historical_taps = taps_df.filter(col("day") < last_week_start_date)
    historical_pays = pays_df.filter(col("pay_date") < last_week_start_date)

    user_prints_history = historical_prints.groupBy("user_id", "value_prop") \
        .agg(count("*").alias("prints_3w_before"))

    user_taps_history = historical_taps.groupBy("user_id", "value_prop") \
        .agg(count("*").alias("taps_3w_before"))

    user_pays_history = historical_pays.groupBy("user_id", "value_prop") \
        .agg(
            count("*").alias("pays_3w_before"),
            sum("total").alias("amount_3w_before")
        )

    # 5. Unir features al DataFrame base
    final_df = base_df.join(user_prints_history, ["user_id", "value_prop"], "left") \
                      .join(user_taps_history, ["user_id", "value_prop"], "left") \
                      .join(user_pays_history, ["user_id", "value_prop"], "left")

    # 6. Rellenar nulos con 0
    final_df = final_df.fillna(0, subset=[
        "prints_3w_before", "taps_3w_before", "pays_3w_before", "amount_3w_before"
    ])

    # 7. Seleccionar y ordenar columnas finales
    final_df = final_df.select(
        "day", "user_id", "position", "value_prop", "was_clicked",
        "prints_3w_before", "taps_3w_before", "pays_3w_before", "amount_3w_before"
    ).orderBy("day", "user_id")

    return final_df

def save_output(df, path):
    """Guarda el DataFrame final en formato Delta Lake."""
    print(f"Guardando dataset final en: {path}")
    df.write.format("delta").mode("overwrite").save(path)
    print("Guardado completado.")

def main():
    """Función principal que orquesta el pipeline."""
    
    # Define las rutas de los archivos de entrada y salida
    # (Asegúrate de que los archivos estén en la misma carpeta que el script, o cambia la ruta)
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_paths = {
        "prints": os.path.join(base_dir, "prints.json"),
        "taps": os.path.join(base_dir, "taps.json"),
        "pays": os.path.join(base_dir, "pays.csv")
    }
    output_path = os.path.join(base_dir, "output_feature_dataset")

    # Ejecución del pipeline
    spark = create_spark_session()
    raw_dataframes = ingest_data(spark, data_paths)
    feature_dataset = engineer_features(raw_dataframes)
    
    print("Muestra del dataset final:")
    feature_dataset.show(10)
    
    save_output(feature_dataset, output_path)
    
    spark.stop()

if __name__ == "__main__":
    main()