import logging
import pandas_gbq
from itertools import product
import os

from src.config.bigquery_config import PROJECT_ID_GBQ, CREDENTIALS_GBQ  
from src.utils.download_data import download_data
from src.utils.setup_logging import setup_logging
from src.config import dir_config, seasons_to_download

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