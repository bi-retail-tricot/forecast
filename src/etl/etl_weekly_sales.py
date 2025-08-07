import logging
import pandas_gbq
from itertools import product
import os
import datetime as dt
import pandas as pd
import polars as pl

from src.config.bigquery_config import PROJECT_ID_GBQ, CREDENTIALS_GBQ 
from src.config import dir_config, seasons_to_download

from src.utils.download_data import download_data
from src.utils.setup_logging import setup_logging

setup_logging()

with open(dir_config.QUERY_TEMPLATE, 'r', encoding='utf-8') as file:
    QUERY_TEMPLATE = file.read()

def generate_query_season(plantilla_sql: str,
                          temporadas: list[str], 
                          anos: list[int]) -> dict[str, str]:
    queries = {}
    for ano in sorted(anos, reverse=True):  # Año descendente
        for temporada in temporadas:        # Orden explícito: Verano, Invierno
            clave = f"{temporada}_{ano}"
            query = plantilla_sql.replace("{nombre_temporada}", f'"{temporada}"').replace("{ano_temporada}", str(ano))
            queries[clave] = query

    return queries

def extract_sales_data(output_dir=None):
    logging.info("Starting data extraction...")
    SEASONS_QUERYS = generate_query_season(
        QUERY_TEMPLATE,
        seasons_to_download.TEMPORADAS,
        seasons_to_download.ANOS
    )

    for season_key, q in SEASONS_QUERYS.items():
        temporada, _ = season_key.split("_")

        # Crear carpeta por temporada
        season_dir = os.path.join(output_dir, temporada)
        os.makedirs(season_dir, exist_ok=True)

        # Guardar archivo dentro de la carpeta de la temporada
        output_path = os.path.join(season_dir, f"weekly_sales_{season_key}.parquet")

        logging.info(f"Descargando {season_key} en {output_path}...")
        download_data(
            query=q,
            output_path=output_path,
            project_id=PROJECT_ID_GBQ,
            credentials=CREDENTIALS_GBQ,
            fast_download=True
        )

def optimize_dataframe(df: pl.DataFrame) -> pl.DataFrame:
    """
    Optimiza tipos de datos para reducir uso de memoria sin perder precisión.
    """
    logging.info("Optimizing dataframe...")
    
    # Definir mapeo de columnas y tipos
    unit_columns = ['weekly_sales', 'stock_start_week', 'stock_end_week']
    money_columns = ['mnt_venta_neta', 'mnt_costo_venta']
    code_map = {
        "cod_sucursal": pl.UInt16,
        "cod_producto": pl.UInt32,
        "cod_talla": pl.UInt16,
        "cod_sku": pl.UInt32,
        "cod_ano_comercial": pl.UInt16,
        "cod_semana": pl.UInt8,
        "mnt_precio_base": pl.UInt32,
        "mnt_precio_vigente": pl.UInt32,
    }
    
    # Crear lista de expresiones para aplicar todas las transformaciones de una vez
    expressions = []
    
    # Unidades (clip y cast a int16)
    for col in unit_columns:
        if col in df.columns:
            expressions.append(pl.col(col).clip(lower_bound=0).cast(pl.Int16))
    
    # Montos (clip, round y cast a float32)
    for col in money_columns:
        if col in df.columns:
            expressions.append(pl.col(col).clip(lower_bound=0).round(3).cast(pl.Float32))
    
    # Códigos (cast directo)
    for col, dtype in code_map.items():
        if col in df.columns:
            expressions.append(pl.col(col).cast(dtype))
    
    # Aplicar todas las transformaciones de una vez
    if expressions:
        df = df.with_columns(expressions)
    
    return df

def sort_partition(df: pl.DataFrame) -> pl.DataFrame:
    """Ordenamiento de datos"""
    logging.info("Sorting partition...")
    return df.sort(['cod_sucursal', 'cod_producto', 'cod_talla', 'cod_ano_comercial', 'cod_semana'])

def calculate_week_number(df: pl.DataFrame) -> pl.DataFrame:
    """Calcula número de semana relativo"""
    logging.info("Calculating relative week number...")
    df = df.with_columns([
        pl.int_range(pl.len())
        .over(['cod_sucursal', 'cod_producto', 'cod_talla'])
        .add(1)
        .cast(pl.UInt8)
        .alias('week_number')
    ])
    return df

def calculate_reposition(df: pl.DataFrame) -> pl.DataFrame:
    """Calcula reposición"""
    logging.info("Calculating reposition...")
    df = df.with_columns([
        (pl.col('stock_end_week') - pl.col('stock_start_week') + pl.col('weekly_sales'))
        .clip(lower_bound=0)
        .cast(pl.UInt16)
        .alias('reposition')
    ])
    return df

def calculate_weekly_available_stock(df: pl.DataFrame) -> pl.DataFrame:
    """Calcula stock disponible semanal"""
    logging.info("Calculating weekly available stock...")
    df = df.with_columns([
        (pl.col('stock_start_week') + pl.col('reposition'))
        .cast(pl.Int16)
        .alias('weekly_available_stock')
    ])
    return df

def calculate_cumulative_sales(df: pl.DataFrame) -> pl.DataFrame:
    """Calcula ventas acumulativas"""
    logging.info("Calculating cumulative sales...")
    df = df.with_columns([
        pl.col('weekly_sales')
        .cum_sum()
        .over(['cod_sucursal', 'cod_producto', 'cod_talla'])
        .cast(pl.Int16)
        .alias('cumulative_sales')
    ])
    return df

def add_week_flags(df: pl.DataFrame) -> pl.DataFrame:
    """Agrega banderas de semana"""
    logging.info("Adding week flags...")
    df = df.with_columns([
        (pl.col('week_number') == 1).cast(pl.UInt8).alias('flag_sku_arrival'),
        (pl.col('weekly_sales') > 0).cast(pl.UInt8).alias('flag_sale'),
        (pl.col('weekly_available_stock') > 0).cast(pl.UInt8).alias('flag_inventory_available'),
        (pl.col('reposition') > 0).cast(pl.UInt8).alias('flag_repo'),
        ((pl.col('weekly_sales') > 0) & (pl.col('stock_end_week') == 0))
        .cast(pl.UInt8).alias('flag_stockout'),
        (pl.col('cumulative_sales') == 0).cast(pl.UInt8).alias('flag_without_first_sale')
    ])
    return df

def calculate_past_rolling_window(df: pl.DataFrame) -> pl.DataFrame:
   """
   Calcula promedio de ventas de las últimas 4 semanas, evitando lookahead bias.
   Suma explícita de las últimas 4 semanas dividida por 4.
   """
   logging.info("Calculating past rolling window...")
   df = df.with_columns([
       pl.col('weekly_sales')
       .shift(1)
       .rolling_sum(window_size=4, min_periods=1)
       .truediv(4)
       .round(3)
       .over(['cod_sucursal', 'cod_producto', 'cod_talla'])
       .alias('mean_sales_past_4_weeks')
   ])
   return df

def calculate_next_rolling_window(df: pl.DataFrame) -> pl.DataFrame:
   """
   Calcula el promedio de ventas de la semana actual + 3 siguientes
   por cada combinación de sucursal, producto y talla.
   """
   logging.info("Calculating next rolling window...")

   df = (
       df.sort(["cod_sucursal", "cod_producto", "cod_talla", "week_number"])
       .with_columns([
           (pl.col("weekly_sales").fill_null(0) + 
            pl.col("weekly_sales").shift(-1).fill_null(0) + 
            pl.col("weekly_sales").shift(-2).fill_null(0) + 
            pl.col("weekly_sales").shift(-3).fill_null(0)
           ).truediv(4)
           .round(3)
           .alias("mean_sales_next_4_weeks")
           .over(["cod_sucursal", "cod_producto", "cod_talla"])
       ])
   )

   return df

def process_data(df: pl.DataFrame) -> pl.DataFrame:
    """Procesamiento completo de datos"""
    logging.info("Processing data...")
    
    # Aplicar optimizaciones y ordenamiento
    df = optimize_dataframe(df)
    df = sort_partition(df)

    # Calcular columnas adicionales
    df = calculate_week_number(df)
    df = calculate_reposition(df)
    df = calculate_cumulative_sales(df)
    df = calculate_weekly_available_stock(df)
    df = calculate_past_rolling_window(df)
    df = calculate_next_rolling_window(df)

    # Agregar banderas de semana
    df = add_week_flags(df)
    
    logging.info("Data processing completed successfully.")
    return df

def etl_process(input_dir: str, output_dir: str) -> None:
    """Proceso ETL principal"""
    logging.info("Consolidating and optimizing raw data...")

    for root, _, files in os.walk(input_dir):
        for file in sorted(files):
            if file.endswith(".parquet"):
                start_time = dt.datetime.now()

                input_file_path = os.path.join(root, file)
                base_name = os.path.splitext(file)[0]

                # Extraer temporada desde el path relativo
                temporada = os.path.basename(root)

                # Crear carpeta de salida por temporada
                output_season_dir = os.path.join(output_dir, temporada)
                os.makedirs(output_season_dir, exist_ok=True)

                output_file = f"{base_name}_processed.parquet"
                output_file_path = os.path.join(output_season_dir, output_file)

                logging.info(f"Processing files: {file}")
                
                # Leer y procesar datos
                df = pl.read_parquet(input_file_path)
                df = process_data(df)

                # Escribir archivo procesado
                df.write_parquet(output_file_path)

                logging.info(f"File processed, saved on: {output_file}")
                elapsed_time = (dt.datetime.now() - start_time).total_seconds()
                logging.info(f"Processing time: {elapsed_time:.0f} seconds")
            else:
                logging.warning(f"Skipping non-parquet file: {file}")

def etl_weekly_sales(
        load_data: bool,
        process_data: bool,
        raw_dir: str,
        processed_dir: str) -> None:
    """
    Función ETL principal para datos de ventas semanales.
    Args:
        load_data (bool): Whether to download the data.
        process_data (bool): Whether to process the data.
        raw_dir (str): Directory for raw data.
        processed_dir (str): Directory for processed data.
    """
    logging.info("Starting ETL process for weekly sales data...")
    start = dt.datetime.now()
    
    if load_data:
        extract_sales_data(output_dir=raw_dir)
    else:
        logging.info("Skipping data extraction, using existing data...")

    if process_data:
        etl_process(input_dir=raw_dir, output_dir=processed_dir)
    else:
        logging.info("Skipping data processing, using existing processed data...")
    
    total_minutes = (dt.datetime.now() - start).total_seconds() / 60
    logging.info(f"ETL process for weekly sales completed in {total_minutes:.1f} minutes.")