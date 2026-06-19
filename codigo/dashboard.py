"""
DASHBOARD UNIFICADO - Proyecto Final Cloud Computing
=====================================================
Panel de control Streamlit con 2 pestañas:
  1. MONITOR EN VIVO: Consume Kafka y muestra eventos en tiempo real
  2. REPORTES BATCH: Lee CSVs de S3 (generados por Spark)

Uso:
    streamlit run dashboard.py
"""

import streamlit as st
import json
import time
import pandas as pd
import altair as alt
from kafka import KafkaConsumer
import boto3
from io import StringIO

# ==============================================================================
# CONFIGURACIÓN - Modificar con tus valores de AWS
# ==============================================================================
KAFKA_BROKERS = [
    'b-1.proyectotaxismsk.gvg71d.c14.kafka.us-east-1.amazonaws.com:9092',
    'b-2.proyectotaxismsk.gvg71d.c14.kafka.us-east-1.amazonaws.com:9092'
]
TOPIC = 'viajes.taxi'
S3_BUCKET = 'proyecto-taxis-cloud-2026'
S3_KEY_ZONAS = 'reportes/zonas_rentables'
S3_KEY_STATS = 'reportes/estadisticas'
MAX_ROWS = 200

# ==============================================================================
# CONFIGURACIÓN STREAMLIT
# ==============================================================================
st.set_page_config(
    page_title="Monitor Taxis NYC - Cloud",
    layout="wide",
    initial_sidebar_state="collapsed"
)

st.markdown("""
    <style>
        .block-container { padding-top: 1.5rem; }
        div[data-testid="metric-container"] {
            background-color: #262730;
            border: 1px solid #444;
            padding: 10px;
            border-radius: 8px;
            text-align: center;
        }
    </style>
""", unsafe_allow_html=True)

# ==============================================================================
# ESTADO
# ==============================================================================
if 'data_buffer' not in st.session_state:
    st.session_state['data_buffer'] = []
if 'total_ingresos' not in st.session_state:
    st.session_state['total_ingresos'] = 0.0
if 'total_viajes' not in st.session_state:
    st.session_state['total_viajes'] = 0

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

# ==============================================================================
# LAYOUT PRINCIPAL
# ==============================================================================
def main():
    st.title("SISTEMA DE MONITOREO - TAXIS NYC")
    st.caption("Proyecto Cloud Computing | AWS MSK + EMR + S3 + Streamlit")

    tab_monitor, tab_batch = st.tabs(["📡 MONITOR EN VIVO", "📊 REPORTES BATCH"])

    # =========================================================================
    # PESTAÑA 1: MONITOR EN VIVO (Kafka)
    # =========================================================================
    with tab_monitor:
        st.markdown("### Indicadores en Tiempo Real")
        kpi1, kpi2, kpi3, kpi4 = st.columns(4)

        st.markdown("---")
        st.markdown("### Bitácora de Viajes Recientes")
        tabla_placeholder = st.empty()

    # =========================================================================
    # PESTAÑA 2: REPORTES BATCH (S3)
    # =========================================================================
    with tab_batch:
        st.header("Reportes Históricos (Procesados por Spark)")
        st.caption("Datos provenientes del análisis batch en EMR, almacenados en S3.")

        if st.button("🔄 ACTUALIZAR REPORTES DESDE S3"):
            st.rerun()

        st.markdown("---")

        # ----- Estadísticas Generales -----
        st.subheader("Estadísticas Generales")
        df_stats = leer_csv_s3(S3_BUCKET, S3_KEY_STATS)

        if not df_stats.empty:
            s1, s2, s3_, s4 = st.columns(4)
            s1.metric("Total Viajes", f"{int(df_stats['Total_Viajes'].iloc[0]):,}")
            s2.metric("Ingresos Totales", f"${df_stats['Ingresos_Totales'].iloc[0]:,.2f}")
            s3_.metric("Tarifa Promedio", f"${df_stats['Tarifa_Promedio'].iloc[0]:.2f}")
            s4.metric("Distancia Promedio", f"{df_stats['Distancia_Promedio'].iloc[0]:.2f} mi")
        else:
            st.info("Ejecute el job Spark para generar estadísticas.")

        st.markdown("---")

        # ----- Zonas Rentables -----
        st.subheader("Top Zonas Más Rentables")
        df_zonas = leer_csv_s3(S3_BUCKET, S3_KEY_ZONAS)

        if not df_zonas.empty:
            z1, z2, z3_ = st.columns(3)
            z1.metric("Zonas Analizadas", len(df_zonas))
            z2.metric("Ingresos Totales", f"${df_zonas['Ingresos_Totales'].sum():,.2f}")
            z3_.metric("Viajes Totales", f"{int(df_zonas['Total_Viajes'].sum()):,}")

            col_chart, col_table = st.columns([2, 1])

            with col_chart:
                chart = alt.Chart(df_zonas.head(20)).mark_bar().encode(
                    x=alt.X('Ingresos_Totales', title='Ingresos ($)'),
                    y=alt.Y('pickup_location_id:N', sort='-x', title='Zona ID'),
                    color=alt.Color('Total_Viajes:Q', title='Viajes',
                                    scale=alt.Scale(scheme='blues')),
                    tooltip=['pickup_location_id', 'Ingresos_Totales',
                             'Total_Viajes', 'Ticket_Promedio']
                ).properties(height=500)
                st.altair_chart(chart, use_container_width=True)

            with col_table:
                st.dataframe(
                    df_zonas,
                    use_container_width=True,
                    height=500
                )
        else:
            st.info("Ejecute el job Spark para generar el reporte de zonas.")

    # =========================================================================
    # CONSUMIDOR KAFKA (Monitor en vivo)
    # =========================================================================
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=KAFKA_BROKERS,
            auto_offset_reset='latest',
            group_id='dashboard-monitor-cloud',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
    except Exception:
        with tab_monitor:
            st.error("No se pudo conectar a Kafka. Verifique la configuración.")
        st.stop()

    # Bucle de actualización en vivo
    for message in consumer:
        viaje = message.value

        precio = float(viaje.get('fare_amount', 0.0))
        distancia = float(viaje.get('trip_distance', 0.0))
        origen = viaje.get('pickup_location_id', 0)
        destino = viaje.get('dropoff_location_id', 0)

        st.session_state['total_ingresos'] += precio
        st.session_state['total_viajes'] += 1

        promedio = st.session_state['total_ingresos'] / max(st.session_state['total_viajes'], 1)

        nuevo = {
            "ID": viaje.get('id', '')[:8],
            "HORA": time.strftime("%H:%M:%S"),
            "ORIGEN": origen,
            "DESTINO": destino,
            "TARIFA": precio,
            "DISTANCIA": distancia,
        }

        st.session_state['data_buffer'].insert(0, nuevo)
        if len(st.session_state['data_buffer']) > MAX_ROWS:
            st.session_state['data_buffer'].pop()

        # Actualizar KPIs
        kpi1.metric("Viajes Totales", st.session_state['total_viajes'])
        kpi2.metric("Ingresos Totales", f"${st.session_state['total_ingresos']:,.2f}")
        kpi3.metric("Tarifa Promedio", f"${promedio:.2f}")
        kpi4.metric("Última Tarifa", f"${precio:.2f}")

        # Actualizar tabla
        df = pd.DataFrame(st.session_state['data_buffer'])
        tabla_placeholder.dataframe(df, use_container_width=True, height=400)

        time.sleep(0.05)

if __name__ == "__main__":
    main()
