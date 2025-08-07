import logging
import pandas as pd
from src.utils.setup_logging import setup_logging

setup_logging()

def export_dataframes_as_tables(dataframes_dict, file_path, rename_dict=None):
    """
    Exporta múltiples DataFrames a Excel con formato tabla, autoajuste y sin líneas de cuadrícula.
    Aplica renombramiento de columnas si se proporciona un diccionario.

    Args:
        dataframes_dict: dict con {sheet_name: dataframe}
        file_path: ruta del archivo Excel
        rename_dict: dict opcional con renombramientos de columnas
    """
    for name, df in dataframes_dict.items():
        if df.shape[0] == 0:
            logging.info(f"Warning: DataFrame '{name}' is empty.")
        else:
            logging.info(f"DataFrame '{name}' has {df.shape[0]} rows and {df.shape[1]} columns.")

    with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
        for sheet_name, df in dataframes_dict.items():
            # Aplicar renombramiento si se proporciona
            df_export = df.rename(columns=rename_dict) if rename_dict else df.copy()

            # Escribir el DataFrame
            df_export.to_excel(writer, sheet_name=sheet_name, index=False)

            # Obtener la hoja
            worksheet = writer.sheets[sheet_name]
            (max_row, max_col) = df_export.shape

            # Ocultar líneas de cuadrícula
            worksheet.hide_gridlines(2)

            # Formato tabla
            worksheet.add_table(0, 0, max_row, max_col - 1, {
                'columns': [{'header': col} for col in df_export.columns],
                'style': 'Table Style Medium 2'
            })

            # Autoajustar ancho de columnas
            for i, col in enumerate(df_export.columns):
                max_len = max(
                    len(str(col)),
                    df_export[col].astype(str).str.len().max() if len(df_export) > 0 else 0
                )
                worksheet.set_column(i, i, min(max_len + 2, 50))