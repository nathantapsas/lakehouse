import os
from typing import Any
from pathlib import Path

PROJECT_NAME = "dataphile"

AT_HOME = os.getenv("USER") == "nathantapsas" or os.getenv("USERNAME") == "nathantapsas"

# Paths
PROJECT_ROOT_DIR = Path(__file__).parent.parent.parent.resolve()

PACKAGE_CACHE_DIR = PROJECT_ROOT_DIR / ".cache"

DUCKLAKE_DATA_DIR = PROJECT_ROOT_DIR / "data"
DUCKLAKE_STORAGE_DIR = DUCKLAKE_DATA_DIR / "storage"

DUCKLAKE_CATALOG_NAME = "dataphile_catalog"
DUCKLAKE_CATALOG_DB_PATH = DUCKLAKE_DATA_DIR / "catalog.ducklake"
DUCKLAKE_STAGING_DIR = DUCKLAKE_DATA_DIR / "staging"
STATE_DB_PATH = DUCKLAKE_DATA_DIR / "state.duckdb"
DUCK_DB_ATTACH_SQL = f"ATTACH '{DUCKLAKE_CATALOG_DB_PATH}' AS {DUCKLAKE_CATALOG_NAME} (TYPE DUCKLAKE, DATA_PATH '{DUCKLAKE_STORAGE_DIR}');"

RAW_DATA_DIR = PROJECT_ROOT_DIR / "raw_data" # if AT_HOME else Path("\\\\LVAN-FTP1\\default_rpts\\openview")
CONFIG_BASE_DIRECTORY_PATH = PROJECT_ROOT_DIR / "src"/ "etl" / "configs" / "dataphile"
LOG_FOLDER = PROJECT_ROOT_DIR / "logs"

UDF_DIRECTORY = PROJECT_ROOT_DIR / "udfs"

os.makedirs(LOG_FOLDER, exist_ok=True)
os.makedirs(DUCKLAKE_STORAGE_DIR, exist_ok=True)
os.makedirs(DUCKLAKE_STAGING_DIR, exist_ok=True)
# Schemas
SCHEMA_BRONZE = f"bronze_{PROJECT_NAME}"
SCHEMA_SILVER = f"silver_{PROJECT_NAME}"
SCHEMA_GOLD = f"gold_{PROJECT_NAME}"
SCHEMA_AUDIT = f"audit_{PROJECT_NAME}"

# Tables
TABLE_FILE_MANIFEST = "_file_manifest"

# System Columns
SYSTEM_COL_SOURCE_FILE = "__source_file"
SYSTEM_COL_LINE_NUMBER = "__line_number"
SYSTEM_COL_RAW_LINE_CONTENT = "__raw_line_content"
SYSTEM_COL_DATA_SNAPSHOT_DATE = "__data_snapshot_date"
SYSTEM_COL_INGESTED_AT = "__ingested_at"


# Logging Configuration

LOGGING_CONFIG: dict[str, Any] = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "format": "%(asctime)s [%(levelname)s] %(message)s"
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "standard",
            "level": "INFO",
        },
        "rotating_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "standard",
            "level": "DEBUG",
            "filename": str(LOG_FOLDER / "ingestion.log"),
            "maxBytes": 10485760,  # 10MB
            "backupCount": 5,
            "encoding": "utf8",
        },
    },
    "loggers": {
        "": {  # root logger
            "handlers": ["console", "rotating_file"],
            "level": "DEBUG",
            "propagate": True
        },
    }

}