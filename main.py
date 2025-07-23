import logging
import datetime as dt

from src.etl.data_extraction import extract_sales_data
from src.etl.prepare_data import consolidate_optimized_raw_data
from src.utils.setup_logging import setup_logging
from src.etl.transform_data import process_data
from src.analysis.demand_summary import analyze_demand

from src.config.dir_config import (
    WEEKLY_SALES_DIR,
    OUTPUT_PATH_PROCESSED_WEEKLY_SALES,
    OUTPUT_PATH_RAW_WEEKLY_SALES,
    OUTPUT_PATH_DEMMAND_SUMMARY
)

setup_logging()

DOWNLOAD_DATA = False
CONSOLIDATE_DATA = False
PROCESS_DATA = False
ANALYZE_DEMMAND = True

def main():
    start = dt.datetime.now()
    if DOWNLOAD_DATA:
        extract_sales_data(output_dir = WEEKLY_SALES_DIR)
    else:
        logging.info("Skipping data extraction, using existing data...")

    if CONSOLIDATE_DATA:
        consolidate_optimized_raw_data(input_path=WEEKLY_SALES_DIR,
                                   output_path=OUTPUT_PATH_RAW_WEEKLY_SALES)
    else:
        logging.info("Skipping data consolidation, using existing raw data...")

    if PROCESS_DATA:
        process_data(input_path=OUTPUT_PATH_RAW_WEEKLY_SALES,
                     output_path=OUTPUT_PATH_PROCESSED_WEEKLY_SALES)
    else:
        logging.info("Skipping data processing, using existing processed data...")
    
    if ANALYZE_DEMMAND:
        analyze_demand(input_path=OUTPUT_PATH_PROCESSED_WEEKLY_SALES,
                     output_path=OUTPUT_PATH_DEMMAND_SUMMARY)
    else:
        logging.info("Skipping demand analysis, using existing demand summary...")
    
    total_minutes = (dt.datetime.now() - start).total_seconds() / 60
    logging.info(f"Process completed in {total_minutes:.1f} minutes.")

if __name__ == "__main__":
    main()