import pandas as pd
import os
import logging
from src.utils.setup_logging import setup_logging

setup_logging()

input_path = '/bi/workspace/Projects/Forecast/forecast/data/raw/weekly_data_all'
output_path = '/bi/workspace/Projects|/Forecast/forecast/data/processed/weekly_sales_processed.parquet'

def optimize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Optimiza tipos de datos para reducir uso de memoria sin perder precisión.
    Aplica cast explícito por tipo de variable:
    - Unidades (uint16)
    - Montos (float32, redondeados)
    - Códigos (uint8/uint16/uint32)
    """
    df = df.copy()

    # 1. Columnas de unidades
    unit_columns = ['weekly_sales', 'stock_start_week', 'stock_end_week']
    for col in unit_columns:
        if col in df.columns:
            df[col] = df[col].clip(lower=0).astype("int16")

    # 2. Columnas de dinero
    money_columns = ['mnt_venta_neta', 'mnt_costo_venta']
    for col in money_columns:
        if col in df.columns:
            df[col] = df[col].clip(lower=0).round(3).astype("float32")

    # 3. Columnas de códigos
    code_map = {
        "cod_sucursal": "uint16",
        "cod_producto": "uint32",
        "cod_talla": "uint16",
        "cod_ano_comercial": "uint16",
        "cod_semana": "uint8"
    }
    for col, dtype in code_map.items():
        if col in df.columns:
            df[col] = df[col].astype(dtype)

    return df

def consolidate_optimized_raw_data(input_path: str, output_path: str) -> None:
    """
    Reads and concatenates all .parquet files in the input_path directory,
    and saves the consolidated DataFrame to output_path.
    """
    files = sorted(os.listdir(input_path))
    
    dataframes = []
    for file in files:
        if file.endswith(".parquet"):
            logging.info(f"Reading file: {file}")
            df = pd.read_parquet(os.path.join(input_path, file))
            logging.info(f"     Processing file...")
            df = optimize_dataframe(df)
            logging.info("     Appending to list...")
            dataframes.append(df)
        else:
            logging.warning(f"Skipping non-parquet file: {file}")

    if not dataframes:
        logging.error("No .parquet files found. Nothing to consolidate.")
        return

    logging.info("Concatenating DataFrames...")
    df_all = pd.concat(dataframes, ignore_index=True)

    logging.info(f"Saving consolidated DataFrame to: {output_path}")
    df_all.to_parquet(output_path, index=False)
    logging.info("Consolidation completed successfully.")