import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st


weekly_sales_df = pd.read_parquet("../data/processed/weekly_sales_2025.parquet")

def summarize_weekly_sales(df, grouping_cols=['nombre_temporada', 'ano_temporada', 'date']):
    df = df.groupby(grouping_cols, observed=True).agg(
        sku_with_history=('cod_sku', 'count'),
        sku_with_arrival=('flag_sku_arrival', 'sum'),                        
        sku_with_inventory=('flag_inventory_available', 'sum'),
        sku_with_sales=('flag_sale', 'sum'),
        sku_with_stockout=('flag_stockout', 'sum'),
    ).reset_index()

    df['prop_sku_with_sales'] = (df['sku_with_sales'] / df['sku_with_inventory']).fillna(0).round(3)
    df['prop_sku_with_stockout'] = (df['sku_with_stockout'] / df['sku_with_inventory']).fillna(0).round(3)

    return df

# Procesar datos
summary_df = summarize_weekly_sales(weekly_sales_df)

# Streamlit Dashboard
st.title("Dashboard de Ventas Semanales")

# Sidebar con filtros
st.sidebar.header("Filtros")

# Filtro por temporada
temporadas = summary_df['nombre_temporada'].unique()
temporada_seleccionada = st.sidebar.selectbox(
    "Seleccionar Temporada:",
    options=['Todas'] + list(temporadas),
    index=0
)

# Filtro por año
anos = sorted(summary_df['ano_temporada'].unique())
ano_seleccionado = st.sidebar.selectbox(
    "Seleccionar Año:",
    options=['Todos'] + list(anos),
    index=0
)


# Aplicar filtros
df_filtrado = summary_df.copy()

if temporada_seleccionada != 'Todas':
    df_filtrado = df_filtrado[df_filtrado['nombre_temporada'] == temporada_seleccionada]

if ano_seleccionado != 'Todos':
    df_filtrado = df_filtrado[df_filtrado['ano_temporada'] == ano_seleccionado]

# Métricas principales
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total SKUs con Historial", f"{df_filtrado['sku_with_history'].sum():,}")

with col2:
    st.metric("SKUs con Ventas", f"{df_filtrado['sku_with_sales'].sum():,}")

with col3:
    avg_sales_prop = df_filtrado['prop_sku_with_sales'].mean()
    st.metric("% Promedio Ventas", f"{avg_sales_prop:.1%}")

with col4:
    avg_stockout_prop = df_filtrado['prop_sku_with_stockout'].mean()
    st.metric("% Promedio Stockout", f"{avg_stockout_prop:.1%}")

# Gráficos
st.subheader("Evolución Temporal")

# Gráfico 1: Evolución de SKUs
fig1 = go.Figure()

fig1.add_trace(go.Scatter(
    x=df_filtrado['date'],
    y=df_filtrado['sku_with_sales'],
    mode='lines+markers',
    name='SKUs con Ventas',
    line=dict(color='#1f77b4')
))

fig1.add_trace(go.Scatter(
    x=df_filtrado['date'],
    y=df_filtrado['sku_with_inventory'],
    mode='lines+markers',
    name='SKUs con Inventario',
    line=dict(color='#ff7f0e')
))

fig1.add_trace(go.Scatter(
    x=df_filtrado['date'],
    y=df_filtrado['sku_with_stockout'],
    mode='lines+markers',
    name='SKUs con Stockout',
    line=dict(color='#d62728')
))

fig1.update_layout(
    title="Evolución de SKUs por Semana",
    xaxis_title="Fecha",
    yaxis_title="Cantidad de SKUs",
    hovermode='x unified'
)

st.plotly_chart(fig1, use_container_width=True)

# Gráfico 2: Proporciones
fig2 = make_subplots(
    rows=2, cols=1,
    subplot_titles=('Proporción de SKUs con Ventas', 'Proporción de SKUs con Stockout'),
    vertical_spacing=0.1
)

fig2.add_trace(
    go.Scatter(
        x=df_filtrado['date'],
        y=df_filtrado['prop_sku_with_sales'],
        mode='lines+markers',
        name='% Ventas',
        line=dict(color='#2ca02c')
    ),
    row=1, col=1
)

fig2.add_trace(
    go.Scatter(
        x=df_filtrado['date'],
        y=df_filtrado['prop_sku_with_stockout'],
        mode='lines+markers',
        name='% Stockout',
        line=dict(color='#d62728')
    ),
    row=2, col=1
)

fig2.update_layout(
    height=600,
    title="Evolución de Proporciones por Semana"
)

fig2.update_yaxes(title_text="Proporción", row=1, col=1)
fig2.update_yaxes(title_text="Proporción", row=2, col=1)
fig2.update_xaxes(title_text="Fecha", row=2, col=1)

st.plotly_chart(fig2, use_container_width=True)

# Tabla de datos
st.subheader("Datos Detallados")
st.dataframe(
    df_filtrado.sort_values('date', ascending=False),
    use_container_width=True
)

# Ejecutar con: streamlit run nombre_archivo.py