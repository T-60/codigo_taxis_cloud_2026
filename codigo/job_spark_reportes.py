"""
JOB SPARK - Generador de Reportes Batch
=========================================
Lee los datos archivados en S3 (JSON) y genera 2 reportes CSV:
  1. Zonas más rentables (agrupado por pickup_location)
  2. Estadísticas generales (totales y promedios)

Uso (en el cluster EMR):
    spark-submit job_spark_reportes.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg, round, desc, lit

# ==============================================================================
# CONFIGURACIÓN - Modificar con tus valores de AWS
# ==============================================================================
S3_INPUT = 's3a://proyecto-taxis-cloud-2026/viajes_raw'
S3_OUTPUT_ZONAS = 's3a://proyecto-taxis-cloud-2026/reportes/zonas_rentables'
S3_OUTPUT_STATS = 's3a://proyecto-taxis-cloud-2026/reportes/estadisticas'

# ==============================================================================
# INICIALIZACIÓN SPARK
# ==============================================================================
spark = SparkSession.builder \
    .appName("ReportesBatch_ProyectoCloud") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("[SPARK] Generador de Reportes Batch")
print(f"[CONF]  Input: {S3_INPUT}")
print("=" * 60)

# ==============================================================================
# 1. LEER DATOS DESDE S3
# ==============================================================================
try:
    df = spark.read.json(S3_INPUT)
    total = df.count()
    print(f"[OK] {total} registros leídos desde S3")
except Exception as e:
    print(f"[ERROR] No hay datos en S3: {e}")
    spark.stop()
    exit()

# ==============================================================================
# 2. REPORTE: ZONAS MÁS RENTABLES
# ==============================================================================
print("[PROC] Generando reporte de zonas rentables...")

reporte_zonas = df.groupBy("pickup_location_id") \
    .agg(
        count("*").alias("Total_Viajes"),
        round(sum("fare_amount"), 2).alias("Ingresos_Totales"),
        round(avg("fare_amount"), 2).alias("Ticket_Promedio"),
        round(avg("trip_distance"), 2).alias("Distancia_Promedio")
    ) \
    .orderBy(desc("Ingresos_Totales"))

reporte_zonas.coalesce(1).write.mode("overwrite").option("header", "true").csv(S3_OUTPUT_ZONAS)
print(f"[OK] Reporte zonas guardado en: {S3_OUTPUT_ZONAS}")

# Mostrar top 10 en consola
print("\n--- TOP 10 ZONAS MÁS RENTABLES ---")
reporte_zonas.show(10, truncate=False)

# ==============================================================================
# 3. REPORTE: ESTADÍSTICAS GENERALES
# ==============================================================================
print("[PROC] Generando estadísticas generales...")

stats = df.agg(
    count("*").alias("Total_Viajes"),
    round(sum("fare_amount"), 2).alias("Ingresos_Totales"),
    round(avg("fare_amount"), 2).alias("Tarifa_Promedio"),
    round(avg("trip_distance"), 2).alias("Distancia_Promedio")
)

stats.coalesce(1).write.mode("overwrite").option("header", "true").csv(S3_OUTPUT_STATS)
print(f"[OK] Estadísticas guardadas en: {S3_OUTPUT_STATS}")

# ==============================================================================
# FIN
# ==============================================================================
print("=" * 60)
print("[SPARK] PROCESO TERMINADO EXITOSAMENTE")
print("=" * 60)

spark.stop()
