import pandas as pd
from datetime import date

def add_week_start_date(df,
                        year_col='cod_ano_comercial',
                        week_col='cod_semana',
                        new_col='date'):
    """
    Agrega una columna con la fecha (tipo date) correspondiente al lunes de cada semana
    usando calendario ISO (ISO-8601).
    """
    df[new_col] = df.apply(
        lambda row: date.fromisocalendar(int(row[year_col]), int(row[week_col]), 1),  # 1 = lunes
        axis=1
    )
    return df
