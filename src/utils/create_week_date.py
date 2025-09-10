import pandas as pd
import numpy as np

def add_week_start_date(df, year_col = 'cod_ano_comercial', week_col = 'cod_semana', new_col="week_start_date"):
    """
    Agrega una columna con la fecha (datetime64[D]) correspondiente al lunes
    de cada semana ISO (ISO-8601).
    Si año o semana son nulos → el resultado es NaT.
    """
    # Creamos máscara de filas válidas
    mask_valid = df[year_col].notna() & df[week_col].notna()

    # Inicializamos la columna con NaT
    df[new_col] = pd.NaT

    # Solo construimos iso_dates en las filas válidas
    iso_dates = (
        df.loc[mask_valid, year_col].astype(int).astype(str) + "-" +
        df.loc[mask_valid, week_col].astype(int).astype(str).str.zfill(2) + "-1"
    )

    # Asignamos las fechas calculadas
    df.loc[mask_valid, new_col] = pd.to_datetime(
        iso_dates, format="%G-%V-%u"
    ).values.astype("datetime64[D]")

    return df
