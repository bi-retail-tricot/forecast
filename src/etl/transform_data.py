import pandas as pd
import logging

from src.config.bigquery_config import PROJECT_ID_GBQ, CREDENTIALS_GBQ
from src.config.dir_config import OUTPUT_PATH_DEMMAND_SUMMARY, OUTPUT_PATH_PROCESSED_WEEKLY_SALES
from src.utils.setup_logging import setup_logging
from src.utils.read_data import read_data

setup_logging()

def calculate_reposition(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate the reposition value based on stock and weekly sales.
    The formula is:
    reposition = stock_end_week - stock_start_week + weekly_sales
    The result is clipped to a minimum of 0 and cast to uint16.
    """
    logging.info("Calculating reposition...")

    df['reposition'] = df['stock_end_week'] - df['stock_start_week'] + df['weekly_sales']
    df['reposition'] = df['reposition'].clip(lower=0).astype('uint16')

    return df

def calculate_weekly_available_stock(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate the available inventory at the end of the week.
    The formula is:
    weekly_available_stock = stock_start_week + reposition
    The result is clipped to a minimum of 0 and cast to uint16.
    """
    logging.info("Calculating weekly available stock...")

    df['weekly_available_stock'] = df['stock_start_week'] + df['reposition']
    df['weekly_available_stock'] = df['weekly_available_stock'].astype('int16')

    return df


def add_inventory_flags(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds binary flags related to weekly inventory and sales behavior:
    - flag_sale: there was a sale
    - flag_stock_available: had stock at the beginning or end of the week
    - flag_replenishment: received new inventory
    - flag_stockout_after_sale: sold and ended with zero stock (potential stockout)
    - flag_no_sale_no_stock: no sale and no stock at the beginning of the week
    """
    logging.info("Adding inventory flags...")

    df['flag_sale'] = (df['weekly_sales'] > 0).astype('uint8')

    df['flag_inventory_available'] = (
        (df['weekly_available_stock'] > 0)
    ).astype('uint8')

    df['flag_repo'] = (df['reposition'] > 0).astype('uint8')

    df['flag_stockout'] = (
        (df['weekly_sales'] > 0) & (df['stock_end_week'] == 0)
    ).astype('uint8')

    return df

def add_dimensions(df):
    """
    Agrega dimensiones a un DataFrame usando maestros predefinidos,
    optimizando para memoria y evitando duplicaciones en joins.
    """
    import pandas as pd
    import numpy as np
    import logging

    logging.info("Reading maestros...")

    # Guardar columnas originales para control
    original_cols = df.columns.tolist()

    # Leer maestro de sucursal
    maestro_sucursal = read_data(
        query="""
            SELECT cod_sucursal, nombre_sucursal, tipo_sucursal 
            FROM `bold-momentum-270218.bo_data.maestro_sucursal`
        """,
        project_id=PROJECT_ID_GBQ,
        credentials=CREDENTIALS_GBQ,
        fast_download=False
    )

    # Leer maestro de producto (limpiando duplicados)
    maestro_producto = read_data(
        query="""
            SELECT cod_producto, cod_talla, nombre_temporada, ano_temporada,
                   nombre_depto, nombre_linea, nom_talla
            FROM `bold-momentum-270218.pbi_data.maestro_sku_procesado`
        """,
        project_id=PROJECT_ID_GBQ,
        credentials=CREDENTIALS_GBQ,
        fast_download=True
    )

    # ⚠️ Eliminar duplicados por clave (muy importante para evitar explosión)
    maestro_producto = maestro_producto.drop_duplicates(subset=['cod_producto', 'cod_talla'])

    # Optimizar columnas categóricas ANTES del merge (reduce memoria en join)
    cat_cols = [
        'nombre_temporada', 'ano_temporada', 'nombre_depto',
        'nombre_linea', 'nom_talla', 'nombre_sucursal', 'tipo_sucursal'
    ]

    for col in cat_cols:
        if col in maestro_producto.columns:
            maestro_producto[col] = maestro_producto[col].astype('category')
        if col in maestro_sucursal.columns:
            maestro_sucursal[col] = maestro_sucursal[col].astype('category')

    # ⚙️ Merge defensivo, con control de explosión
    logging.info("Merging maestro_sucursal...")
    df = df.merge(maestro_sucursal, on='cod_sucursal', how='left')

    logging.info("Merging maestro_producto...")
    df = df.merge(maestro_producto, on=['cod_producto', 'cod_talla'], how='left', validate='m:1')

    # Reordenar columnas (opcionales)
    new_cols = [col for col in df.columns if col not in original_cols]
    final_order = original_cols + new_cols  # originales primero
    df = df[final_order]

    # Forzar tipo category para columnas nuevas (post-merge)
    for col in cat_cols:
        if col in df.columns:
            df[col] = df[col].astype('category')

    return df


def process_data(input_path: str,
                 output_path: str) -> pd.DataFrame:
    """
    Process the DataFrame by calculating reposition and optimizing data types.
    """
    logging.info("Processing data...")
    df = pd.read_parquet(input_path)

    df.sort_values(by=['cod_sucursal', 'cod_producto', 'cod_talla', 'cod_ano_comercial','cod_semana'], inplace=True)

    df = calculate_reposition(df)
    
    df = calculate_weekly_available_stock(df)
    df = add_inventory_flags(df)

    df = add_dimensions(df)

    logging.info("Saving processed data to Parquet format...")
    df.to_parquet(output_path, index=False)
    logging.info("Data processing completed successfully.")