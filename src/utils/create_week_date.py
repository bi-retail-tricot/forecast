import pandas as pd
import numpy as np

def add_week_start_date(df,
                        year_col='cod_ano_comercial',
                        week_col='cod_semana',
                        new_col='date'):
    """
    Agrega una columna con la fecha (tipo date) correspondiente al lunes de cada semana
    usando calendario ISO (ISO-8601). Versión optimizada.
    """
    # Creamos una Serie de strings tipo '2024-32-1' (lunes de la semana)
    iso_dates = (
        df[year_col].astype(str) + '-' +
        df[week_col].astype(str).str.zfill(2) + '-1'
    )
    
    # Convertimos usando formato ISO: %G = año ISO, %V = semana ISO, %u = día (1=lunes)
    df[new_col] = pd.to_datetime(iso_dates, format='%G-%V-%u').dt.date
    
    return df