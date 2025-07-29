import numpy as np
import pandas as pd

def croston_calculate(df, sales_col='weekly_sales'):
    """
    Calcula Croston clásico semana a semana, usando solo el pasado (hasta semana t-1).
    Estima:
    - Demanda promedio cuando hay venta (z)
    - Intervalo promedio entre ventas (p)
    - Estimación Croston: z / p

    Requiere dataframe ordenado por fecha para una sola serie SKU–talla–sucursal.
    """
    sales = df[sales_col].values
    intervals = []
    sales_when_positive = []

    last_sale_index = None

    adi_list = []
    z_list = []
    croston_list = []

    for i, v in enumerate(sales):
        # Calcular intervalo solo si hubo venta
        if v > 0:
            if last_sale_index is None:
                interval = np.nan
            else:
                interval = i - last_sale_index
            last_sale_index = i
            sales_when_positive.append(v)
        else:
            interval = np.nan

        intervals.append(interval)

        # Usar solo valores anteriores a la semana actual
        past_intervals = [x for x in intervals[:i] if not np.isnan(x)]
        past_sales = [x for x in sales_when_positive[:len(past_intervals)] if not np.isnan(x)]

        # Promedios acumulados hasta semana t-1
        adi = np.mean(past_intervals) if past_intervals else np.nan
        z = np.mean(past_sales) if past_sales else np.nan
        croston = z / adi if adi and z else np.nan

        adi_list.append(adi)
        z_list.append(z)
        croston_list.append(croston)

    df['interval'] = intervals
    df['adi_croston'] = adi_list
    df['avg_demand_when_sale'] = z_list
    df['croston_estimate'] = croston_list

    df['croston_estimate'] = df['croston_estimate'].fillna(0).round(0).astype(int)

    return df