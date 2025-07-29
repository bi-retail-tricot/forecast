import numpy as np
import pandas as pd

def calculate_tsb_forecast(df, sales_col='weekly_sales', alpha=0.1, beta=0.1):
    """
    Calcula el pronóstico TSB semana a semana solo con datos pasados.
    
    El forecast de cada semana t usa únicamente información hasta t-1.
    """
    sales = df[sales_col].values

    z_list = []
    p_list = []
    tsb_forecast = []

    z = np.nan
    p = np.nan

    for t, y in enumerate(sales):
        # Forecast de t se basa en z y p de t−1
        if np.isnan(z):
            tsb_forecast.append(np.nan)
            z_list.append(np.nan)
            p_list.append(np.nan)

            if y > 0:
                z = y
                p = 1.0
            else:
                z = 0.0
                p = 0.01
        else:
            forecast = z * p
            tsb_forecast.append(forecast)
            z_list.append(z)
            p_list.append(p)

            i = 1 if y > 0 else 0
            if y > 0:
                z = alpha * y + (1 - alpha) * z
            p = beta * i + (1 - beta) * p

    df['tsb_demand'] = z_list
    df['tsb_prob'] = p_list
    df['tsb_forecast'] = tsb_forecast

    df['tsb_forecast'] = df['tsb_forecast'].fillna(0).round(0).astype(int)

    return df