from pathlib import Path

# Root del proyecto forecast/
BASE_DIR = Path(__file__).resolve().parent.parent.parent

# Directorios clave
DATA_DIR = BASE_DIR / "data"
QUERY_DIR = BASE_DIR / "querys"

# Paths a archivos específicos
QUERY_TEMPLATE = QUERY_DIR / "sales_query_template.sql"
WEEKLY_SALES_DIR = DATA_DIR / "raw" / "weekly_sales_all"

OUTPUT_PATH_RAW_WEEKLY_SALES = DATA_DIR / "raw" / "weekly_sales_raw.parquet"
OUTPUT_PATH_PROCESSED_WEEKLY_SALES = DATA_DIR / "processed" / "weekly_sales_processed.parquet"

OUTPUT_PATH_DEMMAND_SUMMARY = DATA_DIR / "processed" / "demand_summary.parquet"