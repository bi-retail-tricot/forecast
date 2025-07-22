import pandas as pd
import logging
import pandas_gbq
import warnings

warnings.simplefilter('ignore', category=pandas_gbq.exceptions.LargeResultsWarning)

def download_data(query,
                  output_path,
                  project_id,
                  credentials,
                  fast_download=False):
    """
    Lee un archivo SQL, ejecuta la consulta en BigQuery y guarda el resultado en un archivo CSV.
    """
    try:
        # Usar pandas_gbq en lugar de bigframes
        logging.info(f"         Ejecutando consulta en BigQuery, descarga rápida: {fast_download}...")
        df = pandas_gbq.read_gbq(
            query, 
            project_id=project_id, 
            credentials=credentials,
            dialect='standard',
            use_bqstorage_api=fast_download
        )
        logging.info(f"         Datos leídos, guardando datos...")
        df.to_parquet(output_path, index=False, compression="snappy")
        logging.info(f"         Datos guardados en: {output_path}")

    except Exception as e:
        logging.info(f"         Error al descargar los datos: {e}")
