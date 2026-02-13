from sqlmesh.core.config import (
    Config,
    ModelDefaultsConfig,
    GatewayConfig,
    DuckDBConnectionConfig,
    LinterConfig
)
from sqlmesh.core.config.connection import DuckDBAttachOptions
from core.settings import (
    SYSTEM_COL_INGESTED_AT,
    DUCKLAKE_CATALOG_DB_PATH, 
    DUCKLAKE_STORAGE_DIR, 
    DUCKLAKE_CATALOG_NAME, 
    STATE_DB_PATH, 
    PACKAGE_CACHE_DIR, 
    SYSTEM_COL_DATA_SNAPSHOT_DATE,
    SYSTEM_COL_SOURCE_FILE
)


config = Config(
    default_gateway="default",
    disable_anonymized_analytics=True,
    model_defaults=ModelDefaultsConfig(
        dialect="duckdb",
        start="2026-01-28",
        cron="@daily"
    ),
    cache_dir=f"{str(PACKAGE_CACHE_DIR)}/.sqlmesh",
    gateways={
        "default": GatewayConfig(
            variables={
                "sys_col_data_snapshot_date": SYSTEM_COL_DATA_SNAPSHOT_DATE,
                "sys_col_ingested_at": SYSTEM_COL_INGESTED_AT,
                "sys_col_source_file": SYSTEM_COL_SOURCE_FILE
            },
            connection=DuckDBConnectionConfig(
                catalogs= { 
                    DUCKLAKE_CATALOG_NAME: DuckDBAttachOptions(
                        type="ducklake", 
                        path=str(DUCKLAKE_CATALOG_DB_PATH), 
                        data_path=str(DUCKLAKE_STORAGE_DIR)
                    )
                },
                extensions=["ducklake"]
            ),
            state_connection=DuckDBConnectionConfig(
                type="duckdb",
                database=str(STATE_DB_PATH)
            )
        )
    },
    # before_all=get_udfs(),
    linter=LinterConfig(
        enabled=True,
        rules={"ambiguousorinvalidcolumn", "invalidselectstarexpansion", "noambiguousprojections"}
    )
)

