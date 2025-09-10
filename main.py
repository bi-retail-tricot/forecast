import logging

from src.utils.setup_logging import setup_logging

from src.config.dir_config import (
    WEEKLY_SALES_RAW_DIR,
    WEEKLY_SALES_PROCESSED_DIR,
    OUTPUT_PATH_DEMAND_SUMMARY
)

from src.etl.etl_weekly_sales import etl_weekly_sales
from src.etl.etl_genex import etl_genex
from src.etl.etl_trf import etl_trf
from src.analysis.demand_summary import process_demand_analysis

setup_logging()


ETL_SALES = False
ETL_GENEX = False
ETL_TRF = False
DEMAND_ANALYSIS = True

def main():
    if ETL_SALES:
        etl_weekly_sales(
            load_data=True,
            process_data=True,
            raw_dir=WEEKLY_SALES_RAW_DIR,
            processed_dir=WEEKLY_SALES_PROCESSED_DIR
        )

    if ETL_GENEX:
        etl_genex(
            load_data=True,
            process_data=True
        )
    if ETL_TRF:
        etl_trf(
            load_data=True,
            process_data=True
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