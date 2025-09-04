import pandas as pd
import numpy as np
import logging
import os
import datetime as dt

from src.utils.setup_logging import setup_logging
from src.utils.add_dimensions import add_dimensions
from src.utils.create_week_date import add_week_start_date

setup_logging()

GROUPING_COLUMNS = [
   'cod_sucursal', 'cod_producto', 'cod_talla', 'cod_sku'
]

def summarize_sales(df: pd.DataFrame) -> pd.DataFrame:
   """Compute mean and std of weekly sales for active sale weeks."""
   logging.info("Summarizing sales data...")
   
   df_sales = df.query('flag_sale == 1')
   
   df_sales = df_sales.groupby(GROUPING_COLUMNS, observed=True).agg(
       total_sales=('weekly_sales', 'sum'),
       mean_sale=('weekly_sales', 'mean'),
       std_sale=('weekly_sales', 'std'),
       mnt_venta_neta_total =('mnt_venta_neta', 'sum'),
       mnt_costo_venta_total =('mnt_costo_venta', 'sum'), 
   ).reset_index()

   df_sales[['mean_sale', 'std_sale']] = df_sales[['mean_sale', 'std_sale']].round(4)
 
   df_sales['mnt_contribucion_total'] = df_sales['mnt_venta_neta_total'] - df_sales['mnt_costo_venta_total']
   df_sales['mg_total']  = (df_sales['mnt_contribucion_total'] / df_sales['mnt_venta_neta_total']).round(4)

   df_sales['PVP'] = (df_sales['mnt_venta_neta_total'] * 1.19 / df_sales['total_sales']).round(2)

   max_week_per_row = df.groupby(GROUPING_COLUMNS)['week_number'].transform('max')

   df_sales['week_number'] = max_week_per_row

   df_sales = df_sales.merge(
         df[GROUPING_COLUMNS + ['week_number','cod_ano_comercial','cod_semana', 'stock_end_week']],
         on=GROUPING_COLUMNS +['week_number'],
         how='left'
   )

   df_sales['total_units'] = df_sales['stock_end_week'] + df_sales['total_sales']

   df_sales['evacuation'] = (df_sales['total_sales'] / df_sales['total_units']).round(3)

   df_sales = add_week_start_date(df_sales, 'cod_ano_comercial', 'cod_semana', new_col='last_week_data')

   df_sales = df_sales.drop(columns=['week_number', 'cod_ano_comercial', 'cod_semana'])

   df_sales['last_week_data'] = pd.to_datetime(df_sales['last_week_data']).values.astype("datetime64[D]")

   return df_sales
   
def summarize_inventory(df: pd.DataFrame) -> pd.DataFrame:
   """Compute mean and std of inventory usando chunks para optimizar memoria."""
   logging.info("Summarizing inventory data...")

   df_inventory = df.query('flag_inventory_available == 1')

   df_inventory = df_inventory.groupby(GROUPING_COLUMNS, observed=True).agg(
       mean_inventory=('weekly_available_stock', 'mean'),
       std_inventory=('weekly_available_stock', 'std'),
       max_inventory=('weekly_available_stock', 'max'),
   ).reset_index()

   return df_inventory

def summarize_reposition(df: pd.DataFrame) -> pd.DataFrame:
    """Compute mean and std of weekly reposition for active reposition weeks."""
    logging.info("Summarizing reposition data...")
    
    df_reposition = df.query('week_number > 2 and flag_repo == 1')
    
    df_reposition = df_reposition.groupby(GROUPING_COLUMNS, observed=True).agg(
         weeks_with_reposition=('reposition', 'count'),
         total_reposition=('reposition', 'sum'),
    ).reset_index()
    
    return df_reposition

def summarize_weeks(df: pd.DataFrame) -> pd.DataFrame:
   """Count number of weeks per SKU and flags of inventory/sales/stockout."""
   logging.info("Summarizing weekly data...")
   df_weeks = df.groupby(GROUPING_COLUMNS, observed=True).agg(
       on_season_weeks=('cod_semana', 'count'),
       available_inventory_weeks=('flag_inventory_available', 'sum'),
       weeks_until_first_sale=('flag_without_first_sale', 'sum'),
       sales_weeks=('flag_sale', 'sum'),
       stockout_weeks=('flag_stockout', 'sum'),
   ).reset_index()

   return df_weeks

def combine_summaries(sales_summary: pd.DataFrame,
                      inventory_summary: pd.DataFrame,
                      weeks_summary: pd.DataFrame,
                      reposition_summary: pd.DataFrame) -> pd.DataFrame:
   """Combine all summaries into a single DataFrame."""
   logging.info("Merging all summaries...")
   df = weeks_summary.merge(sales_summary, on=GROUPING_COLUMNS, how='left')
   df = df.merge(inventory_summary, on=GROUPING_COLUMNS, how='left')
   df = df.merge(reposition_summary, on=GROUPING_COLUMNS, how='left')

   df = df.fillna({
       'total_sales': 0,
       'mean_sale': 0,
       #'std_sale': 0,
       'mean_inventory': 0,
       #'std_inventory': 0,
       'max_inventory': 0,
       'weeks_with_reposition': 0,
       'total_reposition': 0,
       'on_season_weeks': 0,
       'available_inventory_weeks': 0,
       'sales_weeks': 0,
       'stockout_weeks': 0,
       'weeks_until_first_sale': 0
   })

   del sales_summary, inventory_summary, weeks_summary, reposition_summary

   return df

def compute_demand_indicators(df: pd.DataFrame,
                              until_sale = True) -> pd.DataFrame:
   """Compute demand indicators (ADI, CV², etc.)."""
   logging.info("Computing demand indicators...")
   df['ADI'] = np.where(
       df['sales_weeks'] > 0,
       (df['available_inventory_weeks'] - df['weeks_until_first_sale'] * until_sale) / df['sales_weeks'],
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
   
   df['croston_mean_weekly_sales'] = df['mean_sale'] / df['ADI']
   
   df['mean_sales_weeks'] = df['mean_inventory'] / df['croston_mean_weekly_sales']

   return df

def demand_classification_df(df: pd.DataFrame,
                             adi_col = 'ADI',
                             cv2_col = 'CV2_sales') -> pd.DataFrame:
    """
    Classify demand types based on Syntetos and Boylan (2005):
    """
    logging.info("Classifying demand types...")
    def classify_row(row):
        adi = row[adi_col]
        cv2 = row[cv2_col]

        if adi <= 1.32 and cv2 <= 0.49:
            return "Suave"
        elif adi > 1.32 and cv2 <= 0.49:
            return "Intermitente"
        elif adi <= 1.32 and cv2 > 0.49:
            return "Errática"
        else:
            return "Irregular"

    df['demand_type'] = df.apply(classify_row, axis=1)
    df['demand_type'] = pd.Categorical(
        df['demand_type'],
        categories=["Suave", "Errática", "Intermitente", "Irregular"],
        ordered=True
    )

    return df

def demand_data_optimization(df: pd.DataFrame) -> pd.DataFrame:
    """Optimize data types for memory efficiency."""
    logging.info("Optimizing data types for memory efficiency...")
    dtype_optimization_dict = {
        'on_season_weeks': 'uint8',
        'available_inventory_weeks': 'uint8',
        'sales_weeks': 'uint8',
        'stockout_weeks': 'uint8',
        'weeks_until_first_sale': 'uint8',
        'total_sales': 'float32',
        'mean_sale': 'float32',
        'std_sale': 'float32',
        'mean_inventory': 'float32',
        'std_inventory': 'float32',
        'max_inventory': 'float32',
        'weeks_with_reposition': 'uint8',
        'total_reposition': 'float32',
        'ADI': 'float32',
        'CV_sales': 'float32',
        'CV2_sales': 'float32',
        'CV_inventory': 'float32',
        'CV2_inventory': 'float32',
        'croston_mean_weekly_sales': 'float32',
        'mean_sales_weeks': 'float32',
        'demand_type': 'category',
    }

    df = df.astype(dtype_optimization_dict)

    float_cols = df.select_dtypes(include='float').columns
    df[float_cols] = df[float_cols].round(4)

    return df

def analyze_demand(df) -> pd.DataFrame:
   """
   Main function: summarize demand per SKU-talla-sucursal.
   Requires input with weekly flags and sales/inventory data.
   """
   logging.info("Starting demand analysis...")

   sales = summarize_sales(df)

   inventory = summarize_inventory(df)

   weeks = summarize_weeks(df)

   reposition = summarize_reposition(df)

   demand_summary = combine_summaries(sales, inventory, weeks, reposition)
   
   logging.info("Cleaning up memory...")
   del sales, inventory, weeks, reposition

   demand_summary = compute_demand_indicators(demand_summary)

   demand_summary = demand_classification_df(demand_summary)

   demand_summary = demand_data_optimization(demand_summary)

   logging.info("Demand summary completed.")
   
   return demand_summary


def process_demand_analysis(input_dir: str,
                            output_path: str) -> None:
    logging.info("Starting demand analysis process...")
    demand_summary_list = []

    for root, _, files in os.walk(input_dir):  # Recorre recursivamente
        for file in sorted(files):
            if file.endswith(".parquet"):
                input_file_path = os.path.join(root, file)
                logging.info(f"##### Reading file: {input_file_path} #####")
                start_time = dt.datetime.now()

                df = pd.read_parquet(input_file_path)
                demand_summary = analyze_demand(df)
                demand_summary_list.append(demand_summary)

                elapsed_time = (dt.datetime.now() - start_time).total_seconds()
                logging.info(f"Time taken to process {file}: {elapsed_time:.0f} seconds")
            else:
                logging.warning(f"Skipping non-parquet file: {file}")

    if demand_summary_list:
        logging.info("Combining all demand summaries...")
        final_demand_summary = pd.concat(demand_summary_list, ignore_index=True)

        logging.info("Adding dimensions...")
        final_demand_summary = add_dimensions(final_demand_summary)

        logging.info("Saving final demand summary to parquet...")
        final_demand_summary.to_parquet(output_path, index=False)
        logging.info(f"Final demand summary saved to: {output_path}")
    else:
        logging.warning("No valid demand summaries to save.")
    logging.info("Demand analysis process completed.")