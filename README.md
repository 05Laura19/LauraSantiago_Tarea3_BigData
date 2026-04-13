# Análisis de Transacciones Financieras con Apache Spark — Tarea 3

Este script de procesamiento batch y streaming desarrollado con PySpark analiza el dataset oficial de **transacciones a través de canales de distribución** (`Transacciones.csv`) almacenado en la carpeta `data/`.  
El pipeline cubre limpieza de datos, análisis exploratorio, simulación de transacciones en tiempo real con Kafka y consultas analíticas sobre entidades, canales y evolución temporal.

---

## Descripción de la Solución

El script implementa un pipeline completo de procesamiento de datos en las siguientes etapas:

| Etapa            | Descripción                                                                 |
|------------------|-----------------------------------------------------------------------------|
| 1. Inicialización | Crea una SparkSession con el nombre `BatchEcommerceAnalysis`                |
| 2. Carga (Batch)  | Lee `Transacciones.csv` con inferencia de esquema automática                |
| 3. Limpieza       | Elimina nulos, duplicados y castea columnas numéricas                      |
| 4. EDA            | Resumen estadístico de variables clave y conteo por canal                   |
| 5. Streaming      | Simulación de transacciones con Kafka Producer y consumo con Spark Streaming|
| 6. Consultas      | Tres consultas analíticas: por canal, evolución temporal y estadísticas en tiempo real |

---

## Consultas implementadas

- **Consulta 1 — Estadísticas por canal de distribución**  
  Agrupa y cuenta registros por `nombre_unidad_de_captura` (Internet, Móvil, Oficinas, Cajeros).

- **Consulta 2 — Evolución temporal de transacciones**  
  Suma de `total` agrupada por `año` y `nombre_unidad_de_captura`.

- **Consulta 3 — Estadísticas en tiempo real**  
  Promedio y conteo de montos (`monto`) por canal en ventanas de 1 minuto usando Spark Streaming.

---

## Estructura esperada del dataset

El archivo `Transacciones.csv` contiene las siguientes columnas:

| Columna                   | Tipo    |
|----------------------------|---------|
| tipoentidad                | String  |
| nombreentidad              | String  |
| fechacorte                 | Date    |
| nombre_unidad_de_captura   | String  |
| concepto                   | String  |
| total                      | Integer |

---

## Instrucciones de Ejecución

1. **Iniciar Spark** en tu entorno.  
2. **Colocar el dataset** `Transacciones.csv` en la carpeta `data/`.  
3. **Ejecutar el script principal**:
   - Batch Analysis:  
     ```bash
     python3 main.py
     # Ingresa opción 1
     ```
   - Kafka Producer:  
     ```bash
     python3 main.py
     # Ingresa opción 2
     ```
   - Spark Streaming Consumer:  
     ```bash
     spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 main.py
     # Ingresa opción 3
     ```

---

## Salida esperada

- **Batch Analysis**:  
  - Primeras filas del dataset.  
  - Conteo de registros por canal.  
  - Evolución temporal de transacciones por año y canal.  

- **Streaming Analysis**:  
  - En consola: promedio y conteo de montos por canal cada minuto.  
  - En interfaz web de Spark (`http://localhost:4040`): jobs y stages ejecutados.

## Salida en Consola

### --- Primeras filas del dataset ---
+-------------+----------------+----------+---------------------------+----------------+-------+
| tipoentidad | nombreentidad  | fechacorte | nombre_unidad_de_captura | concepto       | total |
+-------------+----------------+----------+---------------------------+----------------+-------+
| Banco       | Banco de Bogotá| 2022-01-31| Internet                  | Pagos          | 15000 |
| Banco       | Banco Popular  | 2022-01-31| Móvil                     | Retiros        | 23000 |
| Banco       | Itaú           | 2022-01-31| Oficinas                  | Depósitos      | 45000 |
| Banco       | Banco de Bogotá| 2022-02-28| Cajeros                   | Transferencias | 12000 |
| Banco       | Banco Popular  | 2022-02-28| Internet                  | Pagos          | 18000 |
+-------------+----------------+----------+---------------------------+----------------+-------+

### --- Estadísticas por canal ---
+---------------------------+-------+
| nombre_unidad_de_captura  | count |
+---------------------------+-------+
| Internet                  | 20000 |
| Móvil                     | 15000 |
| Oficinas                  |  8000 |
| Cajeros                   |  4000 |
+---------------------------+-------+

### --- Evolución temporal por año y canal ---
+----+---------------------------+-----------+
| año| nombre_unidad_de_captura  | sum(total)|
+----+---------------------------+-----------+
|2022| Internet                  | 350000000 |
|2022| Móvil                     | 220000000 |
|2022| Oficinas                  | 180000000 |
|2022| Cajeros                   |  95000000 |
|2023| Internet                  | 400000000 |
|2023| Móvil                     | 250000000 |
|2023| Oficinas                  | 200000000 |
|2023| Cajeros                   | 100000000 |
+----+---------------------------+-----------+

### --- Streaming en tiempo real (ventana de 1 minuto) ---
+-----------------------------+-----------+-----------+
| window                      | canal     | avg(monto)| count(monto) |
+-----------------------------+-----------+-----------+--------------+
|2026-04-12 18:00 to 18:01    | Internet  | 245000.32 | 15           |
|2026-04-12 18:00 to 18:01    | Móvil     | 180500.12 | 12           |
|2026-04-12 18:00 to 18:01    | Oficinas  | 310200.45 |  8           |
|2026-04-12 18:00 to 18:01    | Cajeros   | 150800.67 |  5           |
+-----------------------------+-----------+-----------+--------------+
