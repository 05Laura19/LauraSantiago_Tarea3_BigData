# ============================================
# Tarea 3 - Procesamiento en Batch con PySpark
# Autora: Laura Santiago - UNAD
# ============================================

"""
DESCRIPCIÓN DE LA SOLUCIÓN
--------------------------
Este programa implementa procesamiento en batch utilizando PySpark
para analizar un conjunto de transacciones descargadas de la
Superintendencia Financiera de Colombia (Formato 444).

Se realizan las siguientes tareas:
1. Cargar el dataset desde HDFS.
2. Operaciones de limpieza (eliminar nulos).
3. Transformaciones (normalización de columnas).
4. Análisis exploratorio de datos (EDA) con consultas.
5. Visualización de resultados en consola.

ESTRUCTURA ESPERADA DEL DATASET
-------------------------------
El archivo CSV debe contener al menos las siguientes columnas:
- canal (string): medio de la transacción.
- monto (double): valor de la transacción.
- timestamp (timestamp): fecha y hora de la transacción.
"""

from pyspark.sql import SparkSession, functions as F

# -------------------------------
# Inicializa la sesión de Spark
# -------------------------------
spark = SparkSession.builder.appName("BatchTransacciones").getOrCreate()

# -------------------------------
# Ruta del archivo en HDFS
# -------------------------------
file_path = "hdfs://localhost:9000/Transacciones/Transacciones.csv"

# -------------------------------
# Cargar dataset en modo batch
# -------------------------------
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(file_path)

# -------------------------------
# Exploración inicial
# -------------------------------
print("\n--- Esquema del DataFrame ---")
df.printSchema()

print("\n--- Primeras filas ---")
df.show(5)

# -------------------------------
# Limpieza básica
# -------------------------------
df_clean = df.dropna()

# -------------------------------
# Transformaciones
# -------------------------------
df_transformed = df_clean.withColumn("canal", F.upper(F.col("canal")))

# -------------------------------
# Análisis exploratorio (EDA)
# -------------------------------
print("\n--- Estadísticas básicas ---")
df_transformed.describe().show()

print("\n--- Filtrar valores mayores a 5000 ---")
df_transformed.filter(F.col("monto") > 5000) \
    .select("monto", "timestamp", "canal").show()

print("\n--- Ordenar por monto descendente ---")
df_transformed.orderBy(F.col("monto").desc()).show(10)

print("\n--- Conteo por canal ---")
df_transformed.groupBy("canal").count().show()

# -------------------------------
# Fin del procesamiento en batch
# -------------------------------
spark.stop()