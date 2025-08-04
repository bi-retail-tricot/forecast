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

with open(dir_config.QUERY_GENEX, 'r', encoding='utf-8') as file:
    query_genex = file.read()

download_data(
            query=query_genex,
            output_path=dir_config.GENEX_RAW_PATH,
            project_id=PROJECT_ID_GBQ,
            credentials=CREDENTIALS_GBQ,
            fast_download=False
        )

def optimize_genex(file_name: str, output_path: str) -> pd.DataFrame:
    """
    Carga un CSV, optimiza los tipos de datos y guarda el resultado en formato Parquet.

    Parámetros:
    - file_name: Ruta al archivo CSV.
    - output_path: Ruta donde se guardará el archivo Parquet optimizado.

    Retorna:
    - DataFrame optimizado.
    """
    df = read_csv_log(file_name)

    # Conversión de tipos numéricos
    df = df.astype({
        'cod_sucursal': 'uint8',
        'cod_producto': 'uint32',
        'vta_periodo': 'uint16',
        'vta_promedio': 'float32',
        'semana_vta': 'uint8',
        'ume': 'uint8',
        'factor': 'float32',
        'stock_sucursal': 'uint16',
        'stock_bodega': 'uint16',
        'stock_on_hand': 'uint16',
        'repo_x_ume': 'uint16',
        'repo_x_dda': 'uint16',
        'can_original': 'uint16',
        'can_final': 'uint16',
    })

    # Conversión de fechas
    #df['semana'] = pd.to_datetime(df['semana'], format='%Y%m%d', errors='coerce')

    # Conversión a categorías ordenadas
    df['estado'] = pd.Categorical(df['estado'], ordered=True)
    df['clasif'] = pd.Categorical(df['clasif'], ordered=True)

    logging.info("         Validando formula repo x dda...")
    df["valida_formula_repo"] = (
    ((df["vta_promedio"] * df["factor"] * df["semana_vta"]) + 0.6)
    .astype(int)
    .sub(df["stock_sucursal"])
    .clip(lower=0)
    .eq(df["repo_x_dda"])
    )


    # Guardar en Parquet optimizado
    save_optimized_parquet(df, output_path=output_path, name="data_genex")
    