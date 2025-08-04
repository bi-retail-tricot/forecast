import logging
import pandas_gbq
from itertools import product
import os
import datetime as dt
import pandas as pd

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



def optimize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Optimiza tipos de datos para reducir uso de memoria sin perder precisión.
    Aplica cast explícito por tipo de variable:
    - Unidades (uint16)
    - Montos (float32, redondeados)
    - Códigos (uint8/uint16/uint32)
    """
    unit_columns = ['weekly_sales', 'stock_start_week', 'stock_end_week']
    for col in unit_columns:
        if col in df.columns:
            df[col] = df[col].clip(lower=0).astype("int16")

    money_columns = ['mnt_venta_neta', 'mnt_costo_venta']
    for col in money_columns:
        if col in df.columns:
            df[col] = df[col].clip(lower=0).round(3).astype("float32")

    code_map = {
        "cod_sucursal": "uint16",
        "cod_producto": "uint32",
        "cod_talla": "uint16",
        "cod_sku": "uint32",
        "cod_ano_comercial": "uint16",
        "cod_semana": "uint8"
    }

    for col, dtype in code_map.items():
        if col in df.columns:
            df[col] = df[col].astype(dtype)

    return df

def sort_partition(df):
    df = df.sort_values(by=['cod_sucursal', 'cod_producto', 'cod_talla', 'cod_ano_comercial', 'cod_semana'])

    return df

def calculate_week_number(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Calculating relative week number...")
    df['week_number'] = df.groupby(['cod_sucursal', 'cod_producto', 'cod_talla'], observed=True).cumcount() + 1
    df['week_number'] = df['week_number'].astype('uint8')

    return df

def calculate_reposition(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Calculating reposition...")
    df['reposition'] = df['stock_end_week'] - df['stock_start_week'] + df['weekly_sales']
    df['reposition'] = df['reposition'].clip(lower=0).astype('uint16')

    return df

def calculate_weekly_available_stock(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Calculating weekly available stock...")
    df['weekly_available_stock'] = df['stock_start_week'] + df['reposition']
    df['weekly_available_stock'] = df['weekly_available_stock'].astype('int16')

    return df

def add_week_flags(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Adding week flags...")
    df['flag_sku_arrival'] = (df['week_number'] == 1).astype('uint8')
    df['flag_sale'] = (df['weekly_sales'] > 0).astype('uint8')
    df['flag_inventory_available'] = (df['weekly_available_stock'] > 0).astype('uint8')
    df['flag_repo'] = (df['reposition'] > 0).astype('uint8')
    df['flag_stockout'] = ((df['weekly_sales'] > 0) & (df['stock_end_week'] == 0)).astype('uint8')

    return df

def process_data(df: pd.DataFrame) -> None:
    logging.info("Processing data...")
    
    df = optimize_dataframe(df)
    df = sort_partition(df)
    df = calculate_week_number(df)
    df = calculate_reposition(df)
    df = calculate_weekly_available_stock(df)

    df = add_week_flags(df)
    
    logging.info("Data processing completed successfully.")

    return df

def etl_process(input_dir: str, output_dir: str) -> None:
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
                df = pd.read_parquet(input_file_path)
                df = process_data(df)  # Asegúrate de definir esta función

                df.to_parquet(output_file_path, index=False)

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
    Main ETL function for weekly sales data.
    Args:
        load_data (bool): Whether to download the data.
        process_data (bool): Whether to process the data.
        analyze_demand (bool): Whether to analyze demand.
        raw_dir (str): Directory for raw data.
        processed_dir (str): Directory for processed data.
        demand_summary_path (str): Path for demand summary output.
    """
    logging.info("Starting ETL process for weekly sales data...")
    start = dt.datetime.now()
    if load_data:
        extract_sales_data(output_dir=raw_dir)
    else:
        logging.info("Skipping data extraction, using existing data...")

    if process_data:
        etl_process(input_dir=raw_dir,
                     output_dir=processed_dir)
    else:
        logging.info("Skipping data processing, using existing processed data...")
    
    total_minutes = (dt.datetime.now() - start).total_seconds() / 60
    logging.info(f"ETL process for weekly sales in {total_minutes:.1f} minutes.")