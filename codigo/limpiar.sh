#!/bin/bash
# ============================================================================
# Deja el sistema EN CERO: detiene los procesos y borra todo lo ya procesado
# en S3 (datos crudos, resumenes de Flink y reportes de Spark) y la salida
# local de Flink. Despues de esto, corre  bash ~/arrancar.sh  para empezar
# a acumular limpio desde cero.
# ============================================================================
export AWS_REQUEST_CHECKSUM_CALCULATION=when_required
BUCKET="s3://proyecto-taxis-cloud-2026"

echo ">>> Deteniendo procesos..."
pkill -9 -f "[p]roductor_viajes";   pkill -9 -f "[a]rchivista_kafka_s3"
pkill -9 -f "[j]ob_flink_streaming"; pkill -9 -f "[s]ubir_flink_s3"
pkill -9 -f "[s]treamlit run"
sleep 3

echo ">>> Borrando datos en S3..."
aws s3 rm "$BUCKET/viajes_raw/"     --recursive --only-show-errors
aws s3 rm "$BUCKET/reportes_flink/" --recursive --only-show-errors
aws s3 rm "$BUCKET/reportes/"       --recursive --only-show-errors

echo ">>> Borrando salida local de Flink y la copia local de Spark..."
rm -rf /home/ec2-user/flink_salida/*
rm -rf /home/ec2-user/viajes_local /home/ec2-user/out_zonas /home/ec2-user/out_stats

echo ">>> Verificando que quedo en cero:"
echo "  viajes_raw:     $(aws s3 ls $BUCKET/viajes_raw/ --recursive | wc -l)"
echo "  reportes_flink: $(aws s3 ls $BUCKET/reportes_flink/ --recursive | wc -l)"
echo "  reportes:       $(aws s3 ls $BUCKET/reportes/ --recursive | wc -l)"
echo ""
echo ">>> Listo. Ahora corre:  bash ~/arrancar.sh"
