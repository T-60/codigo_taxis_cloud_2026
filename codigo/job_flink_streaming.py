"""
Job de streaming con Apache Flink (PyFlink - Table API)
=======================================================
Lee los viajes que llegan a Kafka (Amazon MSK) en tiempo real, los agrupa por
zona de origen en ventanas de 20 segundos y escribe los resultados ya agregados.

Es la capa de streaming "pesada" con un motor de verdad: Flink mantiene el
estado de cada ventana, las cierra solo y entrega los agregados calculados.

Flink escribe los resultados en una carpeta local (~/flink_salida) y un
publicador aparte (subir_flink_s3.sh) los sincroniza al Data Lake en S3, en
la carpeta reportes_flink/. Se separa asi porque la mini-cluster local de
PyFlink no carga el plugin de S3; el AWS CLI, en cambio, usa el rol IAM de la
instancia sin problemas. El resultado es el mismo: los agregados de Flink
terminan en S3.

Requisitos en el EC2:
  - apache-flink 1.17.2  (pip install --user apache-flink==1.17.2)
  - Java 11              (Amazon Corretto)
  - El conector Kafka:   ~/flink_jars/flink-sql-connector-kafka-1.17.2.jar

Uso:
  JAVA_HOME=/usr/lib/jvm/java-11-amazon-corretto.x86_64 python3 job_flink_streaming.py
"""

from pyflink.table import EnvironmentSettings, TableEnvironment

# ==============================================================================
# CONFIGURACION
# ==============================================================================
KAFKA_BROKERS = (
    "b-1.proyectotaxismsk.gvg71d.c14.kafka.us-east-1.amazonaws.com:9092,"
    "b-2.proyectotaxismsk.gvg71d.c14.kafka.us-east-1.amazonaws.com:9092"
)
TOPIC = "viajes.taxi"
CONNECTOR_JAR = "file:///home/ec2-user/flink_jars/flink-sql-connector-kafka-1.17.2.jar"
DESTINO = "file:///home/ec2-user/flink_salida/"  # subir_flink_s3.sh lo sincroniza a S3
VENTANA = "20"  # segundos


def main():
    settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(settings)

    conf = t_env.get_config().get_configuration()
    conf.set_string("pipeline.jars", CONNECTOR_JAR)
    conf.set_string("parallelism.default", "1")
    # El sink confirma (cierra) los archivos en cada checkpoint, no antes.
    conf.set_string("execution.checkpointing.interval", "15 s")

    # --------------------------------------------------------------------------
    # Fuente: el topic de Kafka. Cada viaje llega como un JSON.
    # --------------------------------------------------------------------------
    t_env.execute_sql(f"""
        CREATE TABLE viajes (
            id STRING,
            pickup_location_id INT,
            dropoff_location_id INT,
            fare_amount DOUBLE,
            trip_distance DOUBLE,
            momento AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BROKERS}',
            'properties.group.id' = 'flink-streaming-v1',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    # --------------------------------------------------------------------------
    # Destino: archivos JSON en disco local. subir_flink_s3.sh los sube a S3.
    # --------------------------------------------------------------------------
    t_env.execute_sql(f"""
        CREATE TABLE zonas_flink (
            ventana_inicio TIMESTAMP(3),
            ventana_fin TIMESTAMP(3),
            pickup_location_id INT,
            viajes BIGINT,
            ingresos DOUBLE
        ) WITH (
            'connector' = 'filesystem',
            'path' = '{DESTINO}',
            'format' = 'json',
            'sink.rolling-policy.rollover-interval' = '20 s',
            'sink.rolling-policy.check-interval' = '5 s'
        )
    """)

    # --------------------------------------------------------------------------
    # La consulta: ventanas de tiempo por zona de origen.
    # Flink las cierra solo y manda los agregados al sink.
    # --------------------------------------------------------------------------
    t_env.execute_sql(f"""
        INSERT INTO zonas_flink
        SELECT
            TUMBLE_START(momento, INTERVAL '{VENTANA}' SECOND),
            TUMBLE_END(momento, INTERVAL '{VENTANA}' SECOND),
            pickup_location_id,
            COUNT(*),
            SUM(fare_amount)
        FROM viajes
        GROUP BY TUMBLE(momento, INTERVAL '{VENTANA}' SECOND), pickup_location_id
    """).wait()


if __name__ == "__main__":
    main()
