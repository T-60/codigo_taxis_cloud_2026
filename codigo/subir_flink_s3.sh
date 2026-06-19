#!/bin/bash
# ============================================================================
# Publicador de resultados de Flink a S3
# ----------------------------------------------------------------------------
# El job de Flink (job_flink_streaming.py) deja los agregados por ventana en
# ~/flink_salida. Este script los sincroniza cada pocos segundos al Data Lake
# en S3, bajo reportes_flink/. Usa el rol IAM de la instancia (sin llaves).
# ============================================================================
ORIGEN="$HOME/flink_salida/"
DESTINO="s3://proyecto-taxis-cloud-2026/reportes_flink/"

echo "Publicando $ORIGEN -> $DESTINO cada 15s (Ctrl+C para parar)"
while true; do
    aws s3 sync "$ORIGEN" "$DESTINO" --exclude "*.inprogress*" --only-show-errors
    sleep 15
done
