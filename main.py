from logging.config import dictConfig

from src.ingestion.domain import RunContext
from src.ingestion.bundle_layout import BundleLayout
from src.ingestion.orchestrator import Orchestrator, OrchestrationConfig
from src.ingestion.ledger_store import IngestionLedgerStore
from src.core.settings import DUCK_DB_ATTACH_SQL, CONFIG_BASE_DIRECTORY_PATH, DUCKLAKE_STAGING_DIR, LOGGING_CONFIG, RAW_DATA_DIR
from src.ingestion.csv_config import CSVIngestSpec, load_csv_source_configs_from_directory
from src.ingestion.load_planner import DefaultLoadPlanner

dictConfig(LOGGING_CONFIG)

def main():
    csv_ingestion_specs: list[CSVIngestSpec] = load_csv_source_configs_from_directory(str(CONFIG_BASE_DIRECTORY_PATH))
    ingestion_ledger_store = IngestionLedgerStore(duckdb_path=":memory:", ducklake_attach_sql=DUCK_DB_ATTACH_SQL)
    orchestrator = Orchestrator(
        store=ingestion_ledger_store,
        bundle_layout=BundleLayout(staging_root=DUCKLAKE_STAGING_DIR),
        specs=csv_ingestion_specs,
        load_planner=DefaultLoadPlanner({spec.name: spec for spec in csv_ingestion_specs}),
        config=OrchestrationConfig(),
        data_dir=RAW_DATA_DIR,
    )


    orchestrator.run(RunContext(run_id="run_001"))
    
if __name__ == "__main__":
    main()
