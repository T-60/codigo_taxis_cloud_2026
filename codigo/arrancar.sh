#!/bin/bash
# ============================================================================
# Arranca TODO el sistema en el EC2:
#   - productor_viajes.py        (genera viajes -> Kafka)
#   - archivista_kafka_s3.py     (Kafka -> S3, datos crudos)
#   - dashboard.py               (Streamlit, monitor en vivo + reportes)
#   - job_flink_streaming.py     (Apache Flink: Kafka -> ventanas -> local)
#   - subir_flink_s3.sh          (publica la salida de Flink a S3)
# ============================================================================
export AWS_REQUEST_CHECKSUM_CALCULATION=when_required
export JAVA_HOME=/usr/lib/jvm/java-11-amazon-corretto.x86_64
cd /home/ec2-user

# --- limpiar lo anterior ---
pkill -9 -f "[p]roductor_viajes";      pkill -9 -f "[a]rchivista_kafka_s3"
pkill -9 -f "[s]treamlit run";         pkill -9 -f "[j]ob_flink_streaming"
pkill -9 -f "[s]ubir_flink_s3"
sleep 3
mkdir -p /home/ec2-user/flink_salida

# --- streaming en tiempo real + archivado ---
setsid nohup python3 productor_viajes.py        > /tmp/productor.log  2>&1 < /dev/null & disown
setsid nohup python3 archivista_kafka_s3.py      > /tmp/archivista.log 2>&1 < /dev/null & disown
setsid nohup python3 -m streamlit run dashboard.py --server.port 8501 --server.address 0.0.0.0 > /tmp/dashboard.log 2>&1 < /dev/null & disown

# --- Apache Flink (capa de streaming con motor de verdad) + publicador a S3 ---
setsid nohup env JAVA_HOME=$JAVA_HOME python3 job_flink_streaming.py > /tmp/flink.log 2>&1 < /dev/null & disown
setsid nohup bash subir_flink_s3.sh              > /tmp/uploader.log  2>&1 < /dev/null & disown

sleep 12
echo "Productor:  $(ps -eo cmd | grep -c "[p]roductor_viajes") proceso"
echo "Archivista: $(ps -eo cmd | grep -c "[a]rchivista_kafka_s3") proceso"
echo "Flink:      $(ps -eo cmd | grep -c "[j]ob_flink_streaming") proceso (tarda ~1min en escribir a S3)"
echo "Publicador: $(ps -eo cmd | grep -c "[s]ubir_flink_s3") proceso"
echo "Dashboard:  puerto 8501 -> $(ss -tln | grep -c :8501) (1=ok)"
TOK=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 60")
IP=$(curl -s -H "X-aws-ec2-metadata-token: $TOK" http://169.254.169.254/latest/meta-data/public-ipv4)
echo "Listo. Abre http://$IP:8501"
