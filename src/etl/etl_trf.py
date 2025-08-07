import logging
import pandas as pd

from src.config.bigquery_config import PROJECT_ID_GBQ, CREDENTIALS_GBQ 
from src.utils.download_data import download_data
from src.utils.setup_logging import setup_logging
from src.config import dir_config

setup_logging()

with open(dir_config.QUERY_TRF, 'r', encoding='utf-8') as file:
    query_trf = file.read()

def optimize_trf(file_name: str, output_path: str) -> pd.DataFrame:
    """
    Carga un CSV, optimiza los tipos de datos y guarda el resultado en formato Parquet.

    Parámetros:
    - file_name: Ruta al archivo CSV.
    - output_path: Ruta donde se guardará el archivo Parquet optimizado.

    Retorna:
    - DataFrame optimizado.
    """
    logging.info(f"Optimizando archivo: {file_name}")
    df = pd.read_parquet(file_name, engine='pyarrow')

    # Conversión de tipos numéricos
    df = df.astype({
    'cod_sucursal': 'UInt8',
    'cod_producto': 'UInt32',
    'cod_talla': 'UInt8',
    'cod_ano_comercial': 'UInt16',
    'cod_semana': 'UInt8',
    'numero_trf': 'UInt32',
    'nombre_razon': 'category',
    'fecha_apr_ini': 'UInt32',
    'fecha_des_ini': 'UInt32',
    'cantidad_apr': 'UInt16',
    'cantidad_can': 'UInt16',
    'cantidad_des': 'UInt16',
    })

    df = df[df['cantidad_apr'] - df['cantidad_can'] > 0]

    RAZONES_TRF = [
        "PREDISTRIBUIDA",
        "REPOSICION AUTOMATIC",
        "CARGA MANUAL"]
    
    df['nombre_razon_group'] = df['nombre_razon'].apply(lambda x: x if x in RAZONES_TRF else "OTRAS")
    df['nombre_razon_group'] = pd.Categorical(df['nombre_razon_group'], categories=RAZONES_TRF + ["OTRAS"], ordered=True)

    # Guardar en Parquet optimizado
    logging.info(f"Guardando archivo optimizado en: {output_path}")
    df.to_parquet(
        output_path,
        engine='pyarrow',
        compression='snappy',
        index=False
    )

    return logging.info(f"Archivo optimizado guardado en: {output_path}")


def etl_trf(load_data: bool = True,
              process_data: bool = True):
    if load_data:
        download_data(
            query=query_trf,
            output_path=dir_config.TRF_RAW_PATH,
            project_id=PROJECT_ID_GBQ,
            credentials=CREDENTIALS_GBQ,
            fast_download=True,
        )
    else:
        logging.info("Skipping data download for Genex.")

    if process_data:
        optimize_trf(
            file_name=dir_config.TRF_RAW_PATH,
            output_path=dir_config.TRF_PROCESSED_PATH
        )
    else:
        logging.info("Skipping data processing for Genex.")