import pandas as pd
import numpy as np
import logging

from src.utils.setup_logging import setup_logging
from src.etl.transform_data import add_dimensions

setup_logging()

GROUPING_COLUMNS = [
    'cod_sucursal', 'cod_producto', 'cod_talla', 'cod_sku'
]

def summarize_sales(df: pd.DataFrame) -> pd.DataFrame:
    """Compute mean and std of weekly sales for active sale weeks."""
    logging.info("Summarizing sales data...")
    df_sales = df.query('flag_sale == 1').groupby(GROUPING_COLUMNS).agg(
        mean_sale=('weekly_sales', 'mean'),
        std_sale=('weekly_sales', 'std')
    ).reset_index()

    return df_sales


def summarize_inventory(df: pd.DataFrame) -> pd.DataFrame:
    """Compute mean and std of inventory for weeks with available stock."""
    logging.info("Summarizing inventory data...")
    df_inventory = df.query('flag_inventory_available == 1').groupby(GROUPING_COLUMNS).agg(
        mean_inventory=('weekly_available_stock', 'mean'),
        std_inventory=('weekly_available_stock', 'std')
    ).reset_index()

    return df_inventory

def summarize_weeks(df: pd.DataFrame) -> pd.DataFrame:
    """Count number of weeks per SKU and flags of inventory/sales/stockout."""
    logging.info("Summarizing weekly data...")
    df_weeks = df.groupby(GROUPING_COLUMNS).agg(
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
    df = weeks_summary.merge(sales_summary, on=['cod_sucursal', 'cod_producto', 'cod_talla', 'cod_sku'], how='left')
    df = df.merge(inventory_summary, on=['cod_sucursal', 'cod_producto', 'cod_talla', 'cod_sku'], how='left')

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

def demand_classification(adi: float, cv2: float) -> str:
   """Clasifica tipo de demanda según umbrales ADI y CV²"""
   if adi <= 1.32 and cv2 <= 0.49:
       return "Suave"
   elif adi > 1.32 and cv2 <= 0.49:
       return "Intermitente"
   elif adi <= 1.32 and cv2 > 0.49:
       return "Errática"
   else:
       return "Irregular"

def analyze_demand(input_path: str,
                   output_path: str) -> pd.DataFrame:
    """
    Main function: summarize demand per SKU-talla-sucursal.
    Requires input with weekly flags and sales/inventory data.
    """
    logging.info("Starting demand analysis...")
    logging.info(f"Reading data from {input_path}...")
    df = pd.read_parquet(input_path)
    sales = summarize_sales(df)
    inventory = summarize_inventory(df)
    weeks = summarize_weeks(df)
    
    demand_summary = compute_demand_indicators(sales, inventory, weeks)

    logging.info("Classifying demand types...")
    demand_summary['demand_type'] = demand_summary.apply(
        lambda row: demand_classification(row['ADI'], row['CV2_sales']), axis=1
    )

    logging.info("Adding dimensions")
    demand_summary = add_dimensions(demand_summary)

    logging.info(f"Saving demand summary to {output_path}...")
    demand_summary.to_parquet(output_path, index=False)
    logging.info("Demand summary completed.")
    
    return demand_summary