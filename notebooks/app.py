import streamlit as st
import plotly.express as px
import pandas as pd
import datetime as dt

# --- DATOS ---
weekly_sales_gb = pd.read_parquet('../sandbox/weekly_sales_analysis_curva_sizes.parquet')
weekly_sales_gb["week_start_date"] = pd.to_datetime(weekly_sales_gb["week_start_date"])

# --- SIDEBAR FILTROS ---
st.sidebar.header("Filtros")

# 1. Departamento
deptos = weekly_sales_gb['nombre_depto'].unique()
depto = st.sidebar.selectbox("Departamento", sorted(deptos))

# 2. Línea (filtrada por depto)
lineas = weekly_sales_gb.loc[weekly_sales_gb['nombre_depto'] == depto, 'nombre_linea'].unique()
linea = st.sidebar.selectbox("Línea", sorted(lineas))

# 3. Tallaje (filtrado por depto + línea)
tallajes = weekly_sales_gb.loc[
    (weekly_sales_gb['nombre_depto'] == depto) &
    (weekly_sales_gb['nombre_linea'] == linea),
    'sizes'
].unique()
tallaje = st.sidebar.selectbox("Tallaje", sorted(tallajes))

# 4. Talla (filtrada por depto + línea + tallaje, ordenada por cod_talla)
tallas_df = weekly_sales_gb.loc[
    (weekly_sales_gb['nombre_depto'] == depto) &
    (weekly_sales_gb['nombre_linea'] == linea) &
    (weekly_sales_gb['sizes'] == tallaje),
    ['nom_talla', 'cod_talla']
].drop_duplicates().sort_values("cod_talla")

# Mostrar label combinado (cod + nombre) en el selector
tallas_df["label"] = (
    tallas_df["cod_talla"].astype(str) + " - " + tallas_df["nom_talla"].astype(str)
)
talla_label = st.sidebar.selectbox("Talla", tallas_df["label"].tolist())
talla = tallas_df.loc[tallas_df["label"] == talla_label, "nom_talla"].iloc[0]


# --- DATA FILTRADA ---
df = weekly_sales_gb[
    (weekly_sales_gb['nombre_depto'] == depto) &
    (weekly_sales_gb['nombre_linea'] == linea) &
    (weekly_sales_gb['sizes'] == tallaje) &
    (weekly_sales_gb['nom_talla'] == talla)
].copy()

df['curva_sizes'] = df['curva_sizes'].astype(str)

# --- FECHAS IMPORTANTES ---
dia_madre = dt.datetime(2025, 5, 5)
dia_padre = dt.datetime(2025, 6, 9)

# --- GRÁFICO INTERACTIVO ---
fig = px.line(
    df,
    x="week_start_date",
    y="index_service",
    color="curva_sizes",
    markers=True,
    title=f"Índice de servicio por curva de talla<br>Depto: {depto} - Línea: {linea} - Talla: {talla}",
    color_discrete_sequence=px.colors.qualitative.Dark2,  # paleta más agradable
    width=2000,
    height=600
)

# Formato de ejes
fig.update_yaxes(
    title="Índice de servicio",
    tickformat=".0%",
    range=[0, 1]   # siempre de 0 a 1
)

fig.update_xaxes(
    title="Semana",
    tickmode="array",
    tickvals=df["week_start_date"].unique(),
    ticktext=[d.strftime("%d %b") for d in df["week_start_date"].unique()]
)

fig.update_traces(line=dict(width=5))  # ancho

fig.update_layout(
    legend_title_text="Curva de talla",
    legend=dict(
        font=dict(size=14),
        title_font=dict(size=16)
    )
)


# Agregar líneas de referencia
fig.add_vline(x=dia_madre, line_width=2, line_dash="dash", line_color="red")
fig.add_annotation(x=dia_madre, y=df['index_service'].min()-0.05,
                   text="Día de la madre", showarrow=False, font=dict(color="red"))

fig.add_vline(x=dia_padre, line_width=2, line_dash="dash", line_color="red")
fig.add_annotation(x=dia_padre, y=df['index_service'].min()-0.05,
                   text="Día del padre", showarrow=False, font=dict(color="red"))

# Mostrar en Streamlit
st.plotly_chart(fig, use_container_width=True)
