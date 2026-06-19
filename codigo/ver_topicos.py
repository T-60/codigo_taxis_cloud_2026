"""
Inspector de Kafka (MSK) para la demo
=====================================
Como el cluster MSK no expone las herramientas de linea de comandos de Kafka,
este script usa kafka-python para mostrar:
  - la lista de topicos del cluster,
  - el detalle del topico de viajes (particiones),
  - unos cuantos mensajes que estan llegando en vivo.

Uso (en el EC2):  python3 ver_topicos.py
"""
import json
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient

BROKERS = [
    'b-1.proyectotaxismsk.gvg71d.c14.kafka.us-east-1.amazonaws.com:9092',
    'b-2.proyectotaxismsk.gvg71d.c14.kafka.us-east-1.amazonaws.com:9092'
]
TOPIC = 'viajes.taxi'

print("=" * 55)
print("Conectando al cluster MSK (Kafka)...")
admin = KafkaAdminClient(bootstrap_servers=BROKERS)
topicos = admin.list_topics()
print(f"\nTOPICOS EN EL CLUSTER: {topicos}")

consumer = KafkaConsumer(bootstrap_servers=BROKERS)
particiones = consumer.partitions_for_topic(TOPIC)
print(f"\nTopico '{TOPIC}' -> particiones: {sorted(particiones) if particiones else 'no encontrado'}")

print(f"\nLeyendo 5 viajes en vivo del topico '{TOPIC}'...\n")
c = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BROKERS,
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)
n = 0
for m in c:
    v = m.value
    print(f"  particion {m.partition} | zona {v['pickup_location_id']} -> "
          f"{v['dropoff_location_id']} | ${v['fare_amount']}")
    n += 1
    if n >= 5:
        break
print("\nListo.")
