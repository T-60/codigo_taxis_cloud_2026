#!/bin/bash
# ============================================================================
# Genera los reportes BATCH con Spark sobre TODO lo acumulado en S3.
# Pasos: sincroniza los datos crudos de S3 a local, corre Spark y sube los
# CSV resultantes a S3 (carpeta reportes/). El dashboard los lee de ahi.
# Correr cuando quieras refrescar el reporte batch (p.ej. antes de la demo).
# ============================================================================
export AWS_REQUEST_CHECKSUM_CALCULATION=when_required
BUCKET="s3://proyecto-taxis-cloud-2026"

echo ">>> 1/4 Sincronizando datos crudos de S3 a local..."
mkdir -p ~/viajes_local
# --delete: la copia local refleja EXACTAMENTE lo que hay en S3 ahora
# (sin esto, quedarian datos viejos de corridas anteriores).
aws s3 sync "$BUCKET/viajes_raw/" ~/viajes_local/ --delete --only-show-errors
echo "    archivos locales: $(ls ~/viajes_local | wc -l)"

echo ">>> 2/4 Ejecutando Spark (modo local)..."
python3 ~/job_spark_local.py

echo ">>> 3/4 Subiendo reportes a S3..."
ZK=$(ls ~/out_zonas/part-*.csv | head -1)
SK=$(ls ~/out_stats/part-*.csv | head -1)
aws s3 rm "$BUCKET/reportes/" --recursive --only-show-errors
aws s3 cp "$ZK" "$BUCKET/reportes/zonas_rentables/$(basename "$ZK")" --only-show-errors
aws s3 cp "$SK" "$BUCKET/reportes/estadisticas/$(basename "$SK")" --only-show-errors

echo ">>> 4/4 Listo. Estadisticas en S3:"
K=$(aws s3 ls "$BUCKET/reportes/estadisticas/" | grep part- | awk '{print $4}')
aws s3 cp "$BUCKET/reportes/estadisticas/$K" -
