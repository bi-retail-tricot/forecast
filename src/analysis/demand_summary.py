import pandas as pd
import numpy as np
import logging

from src.utils.setup_logging import setup_logging

setup_logging()

def summarize_sales(df: pd.DataFrame) -> pd.DataFrame:
    """Compute mean and std of weekly sales for active sale weeks."""
    logging.info("Summarizing sales data...")
    df_sales = df.query('flag_sale == 1').groupby(['cod_sucursal', 'cod_producto', 'cod_talla']).agg(
        mean_sale=('weekly_sales', 'mean'),
        std_sale=('weekly_sales', 'std')
    ).reset_index()

    return df_sales


def summarize_inventory(df: pd.DataFrame) -> pd.DataFrame:
    """Compute mean and std of inventory for weeks with available stock."""
    logging.info("Summarizing inventory data...")
    df_inventory = df.query('flag_inventory_available == 1').groupby(
        ['cod_sucursal', 'cod_producto', 'cod_talla']
    ).agg(
        mean_inventory=('weekly_available_stock', 'mean'),
        std_inventory=('weekly_available_stock', 'std')
    ).reset_index()

    return df_inventory

def summarize_weeks(df: pd.DataFrame) -> pd.DataFrame:
    """Count number of weeks per SKU and flags of inventory/sales/stockout."""
    logging.info("Summarizing weekly data...")
    df_weeks = df.groupby(['cod_sucursal', 'cod_producto', 'cod_talla']).agg(
        on_season_weeks=('cod_semana', 'count'),
        aviable_inventory_weeks=('flag_inventory_available', 'sum'),
        sales_weeks=('flag_sale', 'sum'),
        stockout_weeks=('flag_stockout', 'sum')
    ).reset_index()

    return df_weeks

def compute_demand_indicators(
    sales_summary: pd.DataFrame,
    inventory_summary: pd.DataFrame,
    weeks_summary: pd.DataFrame) -> pd.DataFrame:
    """Combine all summaries and compute demand indicators (ADI, CV², etc.)."""
    logging.info("Computing demand indicators...")
    df = weeks_summary.merge(sales_summary, on=['cod_sucursal', 'cod_producto', 'cod_talla'], how='left')
    df = df.merge(inventory_summary, on=['cod_sucursal', 'cod_producto', 'cod_talla'], how='left')

    df['ADI'] = np.where(
        df['sales_weeks'] > 0,
        df['aviable_inventory_weeks'] / df['sales_weeks'],
        np.nan
    )

    df['CV_sales'] = np.where(
        df['mean_sale'] > 0,
        df['std_sale'] / df['mean_sale'],
        np.nan
    )
    df['CV2_sales'] = df['CV_sales'] ** 2

    df['CV_inventory'] = np.where(
        df['mean_inventory'] > 0,
        df['std_inventory'] / df['mean_inventory'],
        np.nan
    )
    df['CV2_inventory'] = df['CV_inventory'] ** 2

    return df


def summarize_demand(df: pd.DataFrame) -> pd.DataFrame:
    """
    Main function: summarize demand per SKU-talla-sucursal.
    Requires input with weekly flags and sales/inventory data.
    """
    logging.info("Starting demand analysis...")
    sales = summarize_sales(df)
    inventory = summarize_inventory(df)
    weeks = summarize_weeks(df)
    
    demand_summary = compute_demand_indicators(sales, inventory, weeks)
    
    return demand_summary