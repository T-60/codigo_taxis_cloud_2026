"""
PRODUCTOR DE VIAJES - Proyecto Final Cloud Computing
=====================================================
Lee el dataset de taxis NYC (parquet) y envía eventos a Kafka (AWS MSK).
Cada evento simula un viaje de taxi en tiempo real.

Uso:
    python3 productor_viajes.py
"""

import json
import time
import uuid
import pandas as pd
from kafka import KafkaProducer

# ==============================================================================
# CONFIGURACIÓN - Modificar con tus valores de AWS
# ==============================================================================
KAFKA_BROKERS = [
    'b-1.proyectotaxismsk.gvg71d.c14.kafka.us-east-1.amazonaws.com:9092',
    'b-2.proyectotaxismsk.gvg71d.c14.kafka.us-east-1.amazonaws.com:9092'
]
TOPIC = 'viajes.taxi'
PARQUET_PATH = 'yellow_tripdata_2024-01.parquet'
INTERVALO_SEGUNDOS = 0.1  # Pausa entre envíos

# ==============================================================================
# CARGA DE DATOS
# ==============================================================================
print("=" * 60)
print("[INIT] Productor de Viajes - NYC Taxi")
print(f"[LOAD] Cargando dataset: {PARQUET_PATH}")

df = pd.read_parquet(PARQUET_PATH)
df = df[['PULocationID', 'DOLocationID', 'trip_distance', 'fare_amount']].dropna().reset_index(drop=True)
print(f"[OK]   {len(df)} registros disponibles")

# ==============================================================================
# CONEXIÓN KAFKA
# ==============================================================================
print(f"[CONN] Conectando a MSK: {KAFKA_BROKERS[0][:40]}...")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
print("[OK]   Conexión establecida")
print("=" * 60)

# ==============================================================================
# BUCLE PRINCIPAL - Enviar viajes a Kafka
# ==============================================================================
indice = 0

try:
    while True:
        fila = df.iloc[indice]

        mensaje = {
            'id': str(uuid.uuid4()),
            'pickup_location_id': int(fila['PULocationID']),
            'dropoff_location_id': int(fila['DOLocationID']),
            'trip_distance': round(float(fila['trip_distance']), 2),
            'fare_amount': round(float(fila['fare_amount']), 2),
            'timestamp': time.time()
        }

        producer.send(TOPIC, mensaje)
        print(f"[>>>] Viaje enviado | ID: {mensaje['id'][:8]}... | "
              f"Zona {mensaje['pickup_location_id']} -> {mensaje['dropoff_location_id']} | "
              f"${mensaje['fare_amount']}")

        indice = (indice + 1) % len(df)
        time.sleep(INTERVALO_SEGUNDOS)

except KeyboardInterrupt:
    print("\n[STOP] Productor detenido.")
    producer.close()
