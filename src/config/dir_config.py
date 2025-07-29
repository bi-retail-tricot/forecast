from pathlib import Path

# Root del proyecto forecast/
BASE_DIR = Path(__file__).resolve().parent.parent.parent

# Directorios clave
DATA_DIR = BASE_DIR / "data"
QUERY_DIR = BASE_DIR / "querys"

# Paths a archivos SQL
QUERY_TEMPLATE = QUERY_DIR / "sales_query_template.sql"

# Directorios de datos
WEEKLY_SALES_RAW_DIR = DATA_DIR / "raw" / "weekly_sales_by_season"
WEEKLY_SALES_PROCESSED_DIR = DATA_DIR / "processed" / "weekly_sales_by_season"

# Paths a archivos procesados
OUTPUT_PATH_DEMAND_SUMMARY = DATA_DIR / "processed" / "demand_summary.parquet"