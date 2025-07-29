import logging
import datetime as dt

from src.etl.data_extraction import extract_sales_data
from src.utils.setup_logging import setup_logging
from src.etl.transform_data import etl_process
from src.analysis.demand_summary import process_demand_analysis

from src.config.dir_config import (
    WEEKLY_SALES_RAW_DIR,
    WEEKLY_SALES_PROCESSED_DIR,
    OUTPUT_PATH_DEMAND_SUMMARY
)

setup_logging()

DOWNLOAD_DATA = False
PROCESS_DATA = True
ANALYZE_DEMAND = True

def main():
    start = dt.datetime.now()
    if DOWNLOAD_DATA:
        extract_sales_data(output_dir = WEEKLY_SALES_RAW_DIR)
    else:
        logging.info("Skipping data extraction, using existing data...")

    if PROCESS_DATA:
        etl_process(input_dir=WEEKLY_SALES_RAW_DIR,
                     output_dir=WEEKLY_SALES_PROCESSED_DIR)
    else:
        logging.info("Skipping data processing, using existing processed data...")

    if ANALYZE_DEMAND:
        process_demand_analysis(input_dir=WEEKLY_SALES_PROCESSED_DIR,
                                output_path=OUTPUT_PATH_DEMAND_SUMMARY)
    else:
        logging.info("Skipping demand analysis, using existing demand summary...")
    
    total_minutes = (dt.datetime.now() - start).total_seconds() / 60
    logging.info(f"Process completed in {total_minutes:.1f} minutes.")

if __name__ == "__main__":
    main()