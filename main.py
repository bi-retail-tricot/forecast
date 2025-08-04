import logging
import pandas as pd
import datetime as dt

from src.utils.setup_logging import setup_logging

from src.config.dir_config import (
    WEEKLY_SALES_RAW_DIR,
    WEEKLY_SALES_PROCESSED_DIR,
    OUTPUT_PATH_DEMAND_SUMMARY
)

from src.etl.etl_weekly_sales import etl_weekly_sales
from src.analysis.demand_summary import process_demand_analysis

setup_logging()


ETL = True
DEMAND_ANALYSIS = True

def main():
    if ETL:
        etl_weekly_sales(
            download_data=True,
            process_data=True,
            raw_dir=WEEKLY_SALES_RAW_DIR,
            processed_dir=WEEKLY_SALES_PROCESSED_DIR
        )
    else:
        logging.info("Skipping ETL process")
    
    if DEMAND_ANALYSIS:
        process_demand_analysis(input_dir=WEEKLY_SALES_PROCESSED_DIR,
                                    output_path=OUTPUT_PATH_DEMAND_SUMMARY)
    else:
        logging.info("Skipping demand analysis.")

if __name__ == "__main__":
    main()
    logging.info("Script executed successfully.")