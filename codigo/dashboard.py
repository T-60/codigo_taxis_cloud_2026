"""
Dashboard del proyecto - Monitoreo de taxis de NYC
===================================================
Panel en Streamlit con tres pestanas:
  1. Monitor en vivo:   consume Kafka (MSK) y muestra los viajes que van llegando.
  2. Streaming Flink:    resumen por zona cada 20 s que calcula Apache Flink (desde S3).
  3. Reportes batch:     los CSV que Spark deja en S3 sobre todo el historial.

El monitor en vivo usa st.fragment(run_every="1s"): se refresca solo cada
segundo SIN bloquear el resto de la app, asi los botones y el cambio de pestana
siempre responden.

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
# CONFIGURACION
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
# ESTADO (persiste entre refrescos)
# ==============================================================================
st.session_state.setdefault('data_buffer', [])
st.session_state.setdefault('total_ingresos', 0.0)
st.session_state.setdefault('total_viajes', 0)
st.session_state.setdefault('ultima_tarifa', 0.0)


# ==============================================================================
# CONEXIONES Y LECTURAS
# ==============================================================================
@st.cache_resource
def crear_consumer():
    """Crea un unico consumidor de Kafka y lo reutiliza entre refrescos.

    Al ser cache_resource, no se recrea en cada refresco: mantiene su posicion
    en el topic y va leyendo solo los viajes nuevos.
    """
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
        auto_offset_reset='latest',
        group_id=f"dashboard-{uuid.uuid4().hex[:8]}",
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )


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
    """Lee los archivos part-* que deja Flink (un JSON por linea) y los junta."""
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


# ==============================================================================
# PESTANA 1: MONITOR EN VIVO (fragmento que se refresca solo)
# ==============================================================================
@st.fragment(run_every="1s")
def panel_monitor():
    # --- leer los viajes nuevos que haya en Kafka (sin bloquear) ---
    try:
        consumer = crear_consumer()
        lotes = consumer.poll(timeout_ms=200, max_records=100)
        for _, registros in lotes.items():
            for reg in registros:
                viaje = reg.value
                precio = float(viaje.get('fare_amount', 0.0))
                st.session_state['total_ingresos'] += precio
                st.session_state['total_viajes'] += 1
                st.session_state['ultima_tarifa'] = precio
                st.session_state['data_buffer'].insert(0, {
                    "ID": str(viaje.get('id', ''))[:8],
                    "Hora": time.strftime("%H:%M:%S"),
                    "Origen": viaje.get('pickup_location_id', 0),
                    "Destino": viaje.get('dropoff_location_id', 0),
                    "Tarifa": precio,
                    "Distancia": float(viaje.get('trip_distance', 0.0)),
                })
        del st.session_state['data_buffer'][MAX_ROWS:]
    except Exception as e:
        st.error(f"No se pudo leer de Kafka: {e}")

    # --- dibujar indicadores y tabla ---
    viajes = st.session_state['total_viajes']
    ingresos = st.session_state['total_ingresos']
    promedio = ingresos / max(viajes, 1)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Viajes recibidos", f"{viajes:,}")
    c2.metric("Ingresos acumulados", f"${ingresos:,.2f}")
    c3.metric("Tarifa promedio", f"${promedio:.2f}")
    c4.metric("Ultima tarifa", f"${st.session_state['ultima_tarifa']:.2f}")

    st.markdown("---")
    st.caption("Viajes mas recientes (se actualiza solo cada segundo)")
    if st.session_state['data_buffer']:
        st.dataframe(pd.DataFrame(st.session_state['data_buffer']),
                     use_container_width=True, height=380)
    else:
        st.info("Esperando viajes desde Kafka...")


# ==============================================================================
# LAYOUT PRINCIPAL
# ==============================================================================
def main():
    st.title("Monitoreo de taxis de Nueva York")
    st.caption("Proyecto de Cloud Computing - EC2, MSK, Flink, EMR y S3")

    tab_monitor, tab_flink, tab_batch = st.tabs(
        ["Monitor en vivo", "Streaming Flink", "Reportes batch"]
    )

    # -------------------------------------------------------------------------
    # Pestana 1: monitor en vivo
    # -------------------------------------------------------------------------
    with tab_monitor:
        st.subheader("Viajes en tiempo real (desde Kafka)")
        panel_monitor()

    # -------------------------------------------------------------------------
    # Pestana 2: streaming con Apache Flink
    # -------------------------------------------------------------------------
    with tab_flink:
        st.subheader("Resumen en streaming con Apache Flink")
        st.markdown(
            "Apache Flink toma el **mismo flujo** de viajes de Kafka y, **cada 20 segundos**, "
            "cuenta cuantos viajes y cuanto dinero hubo **en cada zona**. "
            "Es un resumen que se arma solo sobre la marcha, sin esperar a tener todos los datos. "
            "Cada resumen se guarda en S3."
        )
        st.button("Actualizar resumen de Flink")
        st.markdown("---")

        df_flink = leer_jsonl_s3(S3_BUCKET, S3_KEY_FLINK)

        if not df_flink.empty:
            ultima = df_flink['ventana_fin'].max()
            df_ult = df_flink[df_flink['ventana_fin'] == ultima]

            st.markdown(f"**Ultimo resumen calculado** (ventana que cerro a las {str(ultima)[11:19]})")
            m1, m2, m3 = st.columns(3)
            m1.metric("Viajes en esa ventana", f"{int(df_ult['viajes'].sum()):,}")
            m2.metric("Ingresos en esa ventana", f"${df_ult['ingresos'].sum():,.2f}")
            m3.metric("Zonas activas", len(df_ult))

            st.markdown("**Zonas con mas viajes en esa ventana**")
            top_zonas = df_ult.sort_values("viajes", ascending=False).head(10)
            chart_zonas = alt.Chart(top_zonas).mark_bar().encode(
                x=alt.X("viajes:Q", title="Viajes"),
                y=alt.Y("pickup_location_id:N", sort="-x", title="Zona"),
                tooltip=["pickup_location_id", "viajes", "ingresos"]
            ).properties(height=280)
            st.altair_chart(chart_zonas, use_container_width=True)

            st.markdown("**Viajes por ventana en el tiempo** (cada punto es una ventana de 20 s)")
            por_ventana = (df_flink.groupby("ventana_fin", as_index=False)["viajes"]
                           .sum().sort_values("ventana_fin").tail(25))
            chart_tiempo = alt.Chart(por_ventana).mark_line(point=True).encode(
                x=alt.X("ventana_fin:N", title="Fin de ventana"),
                y=alt.Y("viajes:Q", title="Viajes en la ventana"),
                tooltip=["ventana_fin", "viajes"]
            ).properties(height=260)
            st.altair_chart(chart_tiempo, use_container_width=True)
        else:
            st.info("Aun no hay resumenes de Flink en S3. El job tarda alrededor de "
                    "un minuto desde que arranca en escribir el primero.")

    # -------------------------------------------------------------------------
    # Pestana 3: reportes batch (S3)
    # -------------------------------------------------------------------------
    with tab_batch:
        st.subheader("Reportes historicos (procesados por Spark)")
        st.caption("Foto del ultimo procesamiento batch con Spark, guardada en S3. "
                   "No cambia en tiempo real: se actualiza al volver a ejecutar el job de Spark.")
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


if __name__ == "__main__":
    main()
