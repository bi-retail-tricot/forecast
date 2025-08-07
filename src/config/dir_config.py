from pathlib import Path

# Root del proyecto forecast/
BASE_DIR = Path(__file__).resolve().parent.parent.parent

# Directorios clave
DATA_DIR = BASE_DIR / "data"
QUERY_DIR = BASE_DIR / "querys"

# Paths a archivos SQL
QUERY_TEMPLATE = QUERY_DIR / "sales_query_template.sql"
QUERY_GENEX= QUERY_DIR / "query_genex.sql"
QUERY_TRF = QUERY_DIR / "query_trf.sql"

# Directorios de datos
WEEKLY_SALES_RAW_DIR = DATA_DIR / "raw" / "weekly_sales_by_season"
WEEKLY_SALES_PROCESSED_DIR = DATA_DIR / "processed" / "weekly_sales_by_season"

# Paths a archivos en bruto
GENEX_RAW_PATH = DATA_DIR / "raw" / "genex_data.parquet"
TRF_RAW_PATH = DATA_DIR / "raw" / "trf_data.parquet"

# Paths a archivos procesados
OUTPUT_PATH_DEMAND_SUMMARY = DATA_DIR / "processed" / "demand_summary.parquet"
GENEX_PROCESSED_PATH = DATA_DIR / "processed" / "genex_data_processed.parquet"
TRF_PROCESSED_PATH = DATA_DIR / "processed" / "trf_data_processed.parquet"