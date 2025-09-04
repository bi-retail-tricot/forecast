def add_dimensions(df):
    """
    Agrega dimensiones a un DataFrame usando maestros predefinidos,
    optimizando para memoria y evitando duplicaciones en joins.
    """
    import pandas as pd
    import numpy as np
    import logging

    logging.info("Reading maestros...")

    from src.config.bigquery_config import PROJECT_ID_GBQ, CREDENTIALS_GBQ
    from src.utils.read_data import read_data

    # Guardar columnas originales para control
    original_cols = df.columns.tolist()

    # Leer maestro de sucursal
    maestro_sucursal = read_data(
        query="""
            SELECT cod_sucursal, nombre_sucursal, nombre_tipo_sucursal,  
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

    # Eliminar duplicados por clave (muy importante para evitar explosión)
    maestro_producto = maestro_producto.drop_duplicates(subset=['cod_producto', 'cod_talla'])

    # Optimizar columnas categóricas ANTES del merge (reduce memoria en join)
    cat_cols = [
        'nombre_temporada', 'ano_temporada', 'nombre_depto',
        'nombre_linea', 'nom_talla', 'nombre_sucursal', 'nombre_tipo_sucursal'
    ]

    for col in cat_cols:
        if col in maestro_producto.columns:
            maestro_producto[col] = maestro_producto[col].astype('category')
        if col in maestro_sucursal.columns:
            maestro_sucursal[col] = maestro_sucursal[col].astype('category')

    # Merge defensivo, con control de explosión
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
