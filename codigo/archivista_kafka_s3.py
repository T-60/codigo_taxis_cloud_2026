"""
ARCHIVISTA KAFKA -> S3 (Python puro)
====================================
Consume eventos de Kafka (viajes.taxi) y los guarda en S3 como JSON.
Alternativa simple al job de Flink que funciona en cualquier máquina.

Uso (en EC2 o EMR):
    nohup python3 archivista_kafka_s3.py > /tmp/archivista.log 2>&1 &
"""

import json
import time
import boto3
from kafka import KafkaConsumer
from datetime import datetime

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================
KAFKA_BROKERS = [
    'b-1.proyectotaxismsk.gvg71d.c14.kafka.us-east-1.amazonaws.com:9092',
    'b-2.proyectotaxismsk.gvg71d.c14.kafka.us-east-1.amazonaws.com:9092'
]
TOPIC = 'viajes.taxi'
S3_BUCKET = 'proyecto-taxis-cloud-2026'
S3_PREFIX = 'viajes_raw'
BATCH_SIZE = 50       # Escribir a S3 cada N mensajes
FLUSH_INTERVAL = 30   # O cada N segundos (lo que ocurra primero)

# ==============================================================================
# INICIALIZACIÓN
# ==============================================================================
print("=" * 60)
print("[ARCHIVISTA] Kafka -> S3")
print(f"[CONF] Topic: {TOPIC}")
print(f"[CONF] Bucket: s3://{S3_BUCKET}/{S3_PREFIX}/")
print(f"[CONF] Batch: cada {BATCH_SIZE} mensajes o {FLUSH_INTERVAL}s")
print("=" * 60)

s3 = boto3.client('s3', region_name='us-east-1')

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKERS,
    auto_offset_reset='earliest',
    group_id='archivista-s3-v1',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    consumer_timeout_ms=5000   # Timeout para no bloquear indefinidamente
)

print("[OK] Conectado a Kafka")

# ==============================================================================
# BUCLE PRINCIPAL
# ==============================================================================
buffer = []
last_flush = time.time()
total_archivados = 0

def flush_to_s3(records):
    """Escribe un lote de registros a S3 como archivo JSON lines."""
    global total_archivados
    if not records:
        return
    
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    key = f"{S3_PREFIX}/viajes_{timestamp}_{len(records)}.json"
    
    body = '\n'.join(json.dumps(r) for r in records)
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body.encode('utf-8'))
    
    total_archivados += len(records)
    print(f"[S3] {len(records)} registros -> s3://{S3_BUCKET}/{key} (total: {total_archivados})")

try:
    while True:
        # Consumir mensajes
        for msg in consumer:
            buffer.append(msg.value)
            
            # Flush si el buffer está lleno
            if len(buffer) >= BATCH_SIZE:
                flush_to_s3(buffer)
                buffer = []
                last_flush = time.time()
        
        # Flush por timeout (aunque no esté lleno el buffer)
        if buffer and (time.time() - last_flush) >= FLUSH_INTERVAL:
            flush_to_s3(buffer)
            buffer = []
            last_flush = time.time()
        
        # Si no hay mensajes, esperar un poco
        time.sleep(2)

except KeyboardInterrupt:
    print("\n[STOP] Detenido por usuario")
    flush_to_s3(buffer)  # Flush final
    print(f"[TOTAL] {total_archivados} registros archivados en S3")
