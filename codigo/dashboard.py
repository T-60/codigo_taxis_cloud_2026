"""
Dashboard del proyecto - Monitoreo de taxis de NYC
===================================================
Panel en Streamlit con dos pestanas:
  1. Monitor en vivo: consume Kafka (MSK) y muestra los viajes que van llegando.
  2. Reportes batch: lee los CSV que Spark deja en S3.

Uso:
    streamlit run dashboard.py
"""

import streamlit as st
import json
import time
import uuid
import pandas as pd
import altair as alt
from kafka import KafkaConsumer
import boto3
from io import StringIO

# ==============================================================================
# CONFIGURACION - Modificar con tus valores de AWS
# ==============================================================================
KAFKA_BROKERS = [
    'b-1.proyectotaxismsk.gvg71d.c14.kafka.us-east-1.amazonaws.com:9092',
    'b-2.proyectotaxismsk.gvg71d.c14.kafka.us-east-1.amazonaws.com:9092'
]
TOPIC = 'viajes.taxi'
S3_BUCKET = 'proyecto-taxis-cloud-2026'
S3_KEY_ZONAS = 'reportes/zonas_rentables'
S3_KEY_STATS = 'reportes/estadisticas'
S3_KEY_FLINK = 'reportes_flink/'
MAX_ROWS = 200

st.set_page_config(page_title="Monitoreo de taxis NYC", layout="wide")

# ==============================================================================
# ESTADO
# ==============================================================================
if 'data_buffer' not in st.session_state:
    st.session_state['data_buffer'] = []
if 'total_ingresos' not in st.session_state:
    st.session_state['total_ingresos'] = 0.0
if 'total_viajes' not in st.session_state:
    st.session_state['total_viajes'] = 0
if 'ultima_tarifa' not in st.session_state:
    st.session_state['ultima_tarifa'] = 0.0
# Cada sesion del navegador usa su propio grupo de Kafka. Asi cada visitante
# recibe TODOS los viajes (todas las particiones) y no compite con los demas.
if 'consumer_group' not in st.session_state:
    st.session_state['consumer_group'] = f"dashboard-{uuid.uuid4().hex[:8]}"


# ==============================================================================
# FUNCIONES AUXILIARES
# ==============================================================================
def leer_csv_s3(bucket, prefix):
    """Lee un CSV desde S3 (busca el archivo part-* dentro de la carpeta)."""
    try:
        s3 = boto3.client('s3')
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        for obj in response.get('Contents', []):
            if obj['Key'].endswith('.csv') and 'part-' in obj['Key']:
                csv_obj = s3.get_object(Bucket=bucket, Key=obj['Key'])
                body = csv_obj['Body'].read().decode('utf-8')
                return pd.read_csv(StringIO(body))
        return pd.DataFrame()
    except Exception as e:
        st.warning(f"No se pudo leer S3: {e}")
        return pd.DataFrame()


def leer_jsonl_s3(bucket, prefix):
    """Lee los archivos part-* que deja Flink (un JSON por linea) y los junta.

    Flink va dejando un archivo por cada rotacion; aqui los leemos todos y los
    devolvemos como una sola tabla de ventanas.
    """
    try:
        s3 = boto3.client('s3')
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        filas = []
        for obj in response.get('Contents', []):
            if 'part-' not in obj['Key']:
                continue
            cuerpo = s3.get_object(Bucket=bucket, Key=obj['Key'])['Body'].read().decode('utf-8')
            for linea in cuerpo.splitlines():
                linea = linea.strip()
                if linea:
                    filas.append(json.loads(linea))
        return pd.DataFrame(filas)
    except Exception as e:
        st.warning(f"No se pudo leer Flink desde S3: {e}")
        return pd.DataFrame()


def pintar_estado_actual(k1, k2, k3, k4, tabla):
    """Dibuja en los indicadores y la tabla lo ultimo que tenemos guardado.

    Lo llamamos apenas se carga la pestana para que, si la pagina se vuelve a
    cargar (por ejemplo al pulsar el boton de reportes), el monitor no aparezca
    vacio mientras esperamos el siguiente viaje de Kafka.
    """
    viajes = st.session_state['total_viajes']
    ingresos = st.session_state['total_ingresos']
    promedio = ingresos / max(viajes, 1)

    k1.metric("Viajes totales", viajes)
    k2.metric("Ingresos totales", f"${ingresos:,.2f}")
    k3.metric("Tarifa promedio", f"${promedio:.2f}")
    k4.metric("Ultima tarifa", f"${st.session_state['ultima_tarifa']:.2f}")

    if st.session_state['data_buffer']:
        tabla.dataframe(
            pd.DataFrame(st.session_state['data_buffer']),
            use_container_width=True, height=400
        )


# ==============================================================================
# LAYOUT PRINCIPAL
# ==============================================================================
def main():
    st.title("Monitoreo de taxis de Nueva York")
    st.caption("Proyecto de Cloud Computing - EC2, MSK, EMR y S3")

    tab_monitor, tab_flink, tab_batch = st.tabs(
        ["Monitor en vivo", "Streaming Flink", "Reportes batch"]
    )

    # -------------------------------------------------------------------------
    # Pestana 1: monitor en vivo (Kafka)
    # -------------------------------------------------------------------------
    with tab_monitor:
        st.subheader("Indicadores en tiempo real")
        kpi1, kpi2, kpi3, kpi4 = st.columns(4)
        k1_ph = kpi1.empty()
        k2_ph = kpi2.empty()
        k3_ph = kpi3.empty()
        k4_ph = kpi4.empty()

        st.markdown("---")
        st.subheader("Viajes recientes")
        tabla_placeholder = st.empty()

        # Mostramos de una vez lo ultimo que sabemos, asi no se ve en blanco.
        pintar_estado_actual(k1_ph, k2_ph, k3_ph, k4_ph, tabla_placeholder)

    # -------------------------------------------------------------------------
    # Pestana 2: streaming con Apache Flink (S3)
    # -------------------------------------------------------------------------
    with tab_flink:
        st.subheader("Agregados en streaming (Apache Flink)")
        st.caption("Apache Flink lee los viajes de Kafka en tiempo real y los agrupa por "
                   "zona en ventanas de 20 segundos. Cada ventana cerrada se guarda en S3. "
                   "A diferencia del batch, aqui Flink calcula sobre el flujo, sin esperar "
                   "a tener todos los datos.")

        st.button("Actualizar agregados de Flink")
        st.markdown("---")

        df_flink = leer_jsonl_s3(S3_BUCKET, S3_KEY_FLINK)

        if not df_flink.empty:
            df_flink = df_flink.sort_values("ventana_fin", ascending=False)

            f1, f2, f3 = st.columns(3)
            f1.metric("Ventanas calculadas", len(df_flink))
            f2.metric("Viajes agregados", f"{int(df_flink['viajes'].sum()):,}")
            f3.metric("Ingresos agregados", f"${df_flink['ingresos'].sum():,.2f}")

            # Viajes por ventana a lo largo del tiempo
            por_ventana = (df_flink.groupby("ventana_fin", as_index=False)["viajes"]
                           .sum().sort_values("ventana_fin").tail(30))
            chart_f = alt.Chart(por_ventana).mark_line(point=True).encode(
                x=alt.X("ventana_fin:N", title="Fin de ventana"),
                y=alt.Y("viajes:Q", title="Viajes en la ventana"),
                tooltip=["ventana_fin", "viajes"]
            ).properties(height=300)
            st.altair_chart(chart_f, use_container_width=True)

            st.markdown("**Ultimas ventanas procesadas**")
            st.dataframe(df_flink.head(50), use_container_width=True, height=380)
        else:
            st.info("Aun no hay ventanas de Flink en S3. El job tarda alrededor de un "
                    "minuto desde que arranca en escribir la primera.")

    # -------------------------------------------------------------------------
    # Pestana 3: reportes batch (S3)
    # -------------------------------------------------------------------------
    with tab_batch:
        st.subheader("Reportes historicos (procesados por Spark)")
        st.caption("Foto del ultimo procesamiento batch con Spark, guardada en S3. "
                   "No cambia en tiempo real: se actualiza al volver a ejecutar el job de Spark.")

        # Al pulsar el boton, Streamlit recarga la pagina y se vuelven a leer
        # los CSV de S3 de mas abajo. No hace falta nada mas.
        st.button("Actualizar reportes desde S3")

        st.markdown("---")

        # ----- Estadisticas generales -----
        st.markdown("**Estadisticas generales**")
        df_stats = leer_csv_s3(S3_BUCKET, S3_KEY_STATS)

        if not df_stats.empty:
            s1, s2, s3_, s4 = st.columns(4)
            s1.metric("Total viajes", f"{int(df_stats['Total_Viajes'].iloc[0]):,}")
            s2.metric("Ingresos totales", f"${df_stats['Ingresos_Totales'].iloc[0]:,.2f}")
            s3_.metric("Tarifa promedio", f"${df_stats['Tarifa_Promedio'].iloc[0]:.2f}")
            s4.metric("Distancia promedio", f"{df_stats['Distancia_Promedio'].iloc[0]:.2f} mi")
        else:
            st.info("Ejecuta el job de Spark para generar las estadisticas.")

        st.markdown("---")

        # ----- Zonas rentables -----
        st.markdown("**Zonas mas rentables**")
        df_zonas = leer_csv_s3(S3_BUCKET, S3_KEY_ZONAS)

        if not df_zonas.empty:
            z1, z2, z3_ = st.columns(3)
            z1.metric("Zonas analizadas", len(df_zonas))
            z2.metric("Ingresos totales", f"${df_zonas['Ingresos_Totales'].sum():,.2f}")
            z3_.metric("Viajes totales", f"{int(df_zonas['Total_Viajes'].sum()):,}")

            col_chart, col_table = st.columns([2, 1])

            with col_chart:
                chart = alt.Chart(df_zonas.head(20)).mark_bar().encode(
                    x=alt.X('Ingresos_Totales', title='Ingresos ($)'),
                    y=alt.Y('pickup_location_id:N', sort='-x', title='Zona'),
                    tooltip=['pickup_location_id', 'Ingresos_Totales',
                             'Total_Viajes', 'Ticket_Promedio']
                ).properties(height=500)
                st.altair_chart(chart, use_container_width=True)

            with col_table:
                st.dataframe(df_zonas, use_container_width=True, height=500)
        else:
            st.info("Ejecuta el job de Spark para generar el reporte de zonas.")

    # -------------------------------------------------------------------------
    # Consumidor de Kafka (alimenta el monitor en vivo)
    # -------------------------------------------------------------------------
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=KAFKA_BROKERS,
            auto_offset_reset='latest',
            group_id=st.session_state['consumer_group'],
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
    except Exception:
        with tab_monitor:
            st.error("No se pudo conectar a Kafka. Revisa la configuracion.")
        st.stop()

    for message in consumer:
        viaje = message.value

        precio = float(viaje.get('fare_amount', 0.0))
        distancia = float(viaje.get('trip_distance', 0.0))
        origen = viaje.get('pickup_location_id', 0)
        destino = viaje.get('dropoff_location_id', 0)

        st.session_state['total_ingresos'] += precio
        st.session_state['total_viajes'] += 1
        st.session_state['ultima_tarifa'] = precio

        nuevo = {
            "ID": viaje.get('id', '')[:8],
            "Hora": time.strftime("%H:%M:%S"),
            "Origen": origen,
            "Destino": destino,
            "Tarifa": precio,
            "Distancia": distancia,
        }

        st.session_state['data_buffer'].insert(0, nuevo)
        if len(st.session_state['data_buffer']) > MAX_ROWS:
            st.session_state['data_buffer'].pop()

        pintar_estado_actual(k1_ph, k2_ph, k3_ph, k4_ph, tabla_placeholder)

        time.sleep(0.05)


if __name__ == "__main__":
    main()
