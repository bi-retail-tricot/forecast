import pandas as pd
import logging

from src.utils.setup_logging import setup_logging

setup_logging()

def calculate_reposition(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate the reposition value based on stock and weekly sales.
    The formula is:
    reposition = stock_end_week - stock_start_week + weekly_sales
    The result is clipped to a minimum of 0 and cast to uint16.
    """
    logging.info("Calculating reposition...")

    df['reposition'] = df['stock_end_week'] - df['stock_start_week'] + df['weekly_sales']
    df['reposition'] = df['reposition'].clip(lower=0).astype('uint16')

    return df

def calculate_weekly_available_stock(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate the available inventory at the end of the week.
    The formula is:
    weekly_available_stock = stock_start_week + reposition
    The result is clipped to a minimum of 0 and cast to uint16.
    """
    logging.info("Calculating weekly available stock...")

    df['weekly_available_stock'] = df['stock_start_week'] + df['reposition']
    df['weekly_available_stock'] = df['weekly_available_stock'].astype('int16')

    return df


def add_inventory_flags(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds binary flags related to weekly inventory and sales behavior:
    - flag_sale: there was a sale
    - flag_stock_available: had stock at the beginning or end of the week
    - flag_replenishment: received new inventory
    - flag_stockout_after_sale: sold and ended with zero stock (potential stockout)
    - flag_no_sale_no_stock: no sale and no stock at the beginning of the week
    """
    logging.info("Adding inventory flags...")

    df['flag_sale'] = (df['weekly_sales'] > 0).astype('uint8')

    df['flag_inventory_available'] = (
        (df['weekly_available_stock'] > 0)
    ).astype('uint8')

    df['flag_repo'] = (df['reposition'] > 0).astype('uint8')

    df['flag_stockout'] = (
        (df['weekly_sales'] > 0) & (df['stock_end_week'] == 0)
    ).astype('uint8')

    return df



def process_data(input_path: str,
                 output_path: str) -> pd.DataFrame:
    """
    Process the DataFrame by calculating reposition and optimizing data types.
    """
    logging.info("Processing data...")
    df = pd.read_parquet(input_path)

    df = calculate_reposition(df)
    
    df = calculate_weekly_available_stock(df)
    df = add_inventory_flags(df)

    logging.info("Saving processed data to Parquet format...")
    df.to_parquet(output_path, index=False)
    logging.info("Data processing completed successfully.")
    
    return df