from src.etl.data_extraction import extract_sales_data
from src.etl.prepare_data import consolidate_optimized_raw_data
from src.config.dir_config import OUTPUT_PATH_WEEKLY_SALES, OUTPUT_PATH_PROCESSED_WEEKLY_SALES, OUTPUT_PATH_RAW_WEEKLY_SALES
from src.utils.setup_logging import setup_logging
from src.etl.transform_data import process_data

import logging

setup_logging()

DOWNLOAD_DATA = False


def main():
    if DOWNLOAD_DATA:
        logging.info("Starting data extraction...")
        extract_sales_data()
    else:
        logging.info("Skipping data extraction, using existing data...")

    logging.info("Consolidating and optimizing raw data...")
    consolidate_optimized_raw_data(input_path=OUTPUT_PATH_WEEKLY_SALES,
                                   output_path=OUTPUT_PATH_RAW_WEEKLY_SALES)
    
    process_data(input_path=OUTPUT_PATH_RAW_WEEKLY_SALES,
                 output_path=OUTPUT_PATH_PROCESSED_WEEKLY_SALES)

if __name__ == "__main__":
    main()