import json
import logging
from pathlib import Path
from typing import Sequence

from ingestion.csv_config import CSVIngestSpec
from ingestion.domain import FileKey, LoadTarget
from core import settings

logger = logging.getLogger(__name__)


class DefaultLoadPlanner:
    """
    Plans loads by reading the bundle manifest.

    Convention:
      - extraction emits "data.parquet"
      - loads into: <catalog>.<target_schema>.<source_name>
    """

    def __init__(self, specs: dict[str, CSVIngestSpec], target_schema: str = "bronze"):
        self.specs = specs
        self.target_schema = target_schema

    def plan_load(self, file_key: FileKey, extracted_bundle_path: Path) -> Sequence[LoadTarget]:
        manifest_path = extracted_bundle_path / "bundle.json"
        if not manifest_path.exists():
            raise FileNotFoundError(f"Bundle manifest missing: {manifest_path}")

        with open(manifest_path, "r") as f:
            manifest = json.load(f)

        if file_key.source_name not in self.specs:
            raise ValueError(f"No configuration spec found for {file_key.source_name}")

        plan: list[LoadTarget] = []
        for artifact in manifest.get("artifacts", []):
            if artifact.get("type") != "data":
                continue
            plan.append(self._build_data_target(file_key, artifact))

        if not plan:
            raise ValueError(f"No loadable artifacts found in manifest: {manifest_path}")

        return plan

    def _build_data_target(self, file_key: FileKey, artifact: dict[str, object]) -> LoadTarget:
        table_name = f"{settings.DUCKLAKE_CATALOG_NAME}.{self.target_schema}.{file_key.source_name}"
        relpath = str(artifact["relpath"])
        sql = f"INSERT INTO {table_name} BY NAME SELECT * FROM read_parquet(?, union_by_name=true);"
        return LoadTarget(target_table_fqn=table_name, artifact_relpath=relpath, insert_sql=sql, bind_params=())
