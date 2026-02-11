from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import re
from typing import Any

import pyarrow as pa

from ingestion.bundle_layout import BundleLayout
from ingestion.csv_config import CSVIngestSpec
from ingestion.domain import DiscoveredFile, ExtractionResult
from ingestion.extract import FastCSVExtractor
from core.settings import (
    SYSTEM_COL_SOURCE_FILE,
    SYSTEM_COL_LINE_NUMBER,
    SYSTEM_COL_DATA_SNAPSHOT_DATE,
    SYSTEM_COL_INGESTED_AT,
)

logger = logging.getLogger(__name__)


SPEC_TYPE_TO_ARROW_TYPE: dict[str, pa.DataType] = {
    "string": pa.string(),
    "integer": pa.int64(),
    "float": pa.float64(),
    "boolean": pa.bool_(),
    "datetime": pa.timestamp("ms"),
    "decimal": pa.decimal128(38, 10),
    "date": pa.date32(),
}

SYSTEM_COLUMNS_ARROW: list[pa.Field] = [
    pa.field(SYSTEM_COL_SOURCE_FILE, pa.string(), nullable=True),
    pa.field(SYSTEM_COL_LINE_NUMBER, pa.int64(), nullable=True),
    pa.field(SYSTEM_COL_DATA_SNAPSHOT_DATE, pa.date32(), nullable=True),
    pa.field(SYSTEM_COL_INGESTED_AT, pa.timestamp("ms"), nullable=False),
]


def get_arrow_schema_from_spec(spec: CSVIngestSpec) -> pa.Schema:
    fields = SYSTEM_COLUMNS_ARROW.copy()
    for column_name, output_type in spec.get_schema().items():
        arrow_type = SPEC_TYPE_TO_ARROW_TYPE.get(output_type, pa.string())
        fields.append(pa.field(column_name, arrow_type, nullable=True))
    return pa.schema(fields)


def _parse_snapshot_date(file_path: Path, spec: CSVIngestSpec) -> Any | None:
    """Extract snapshot date from filename based on regex/format in the spec."""
    if spec.source.filename_date_format and spec.source.filename_date_regex:
        match = re.search(spec.source.filename_date_regex, file_path.name)
        if not match:
            raise ValueError(
                f"No date match in {file_path.name} using {spec.source.filename_date_regex}"
            )
        try:
            return datetime.strptime(match.group(1), spec.source.filename_date_format).date()
        except Exception as e:
            raise ValueError(f"Error parsing date from {file_path.name}: {e}") from e
    return None


def execute_extraction_task(
    spec: CSVIngestSpec,
    discovered: DiscoveredFile,
    tmp_bundle_dir: Path,
    final_bundle_dir: Path,
    bundle_layout: BundleLayout,
) -> ExtractionResult:
    """
    Extract a raw file into a bundle (parquet + manifest).

    Idempotent:
      - If final bundle already exists and is complete, it is reused.
    """
    raw_path = discovered.raw_file_path
    logger.info("Starting extraction: %s", raw_path)

    # Idempotency check
    if bundle_layout.is_complete_bundle_dir(final_bundle_dir):
        manifest_path = bundle_layout.get_bundle_manifest_path(final_bundle_dir)
        with open(manifest_path, "r") as f:
            manifest = json.load(f)
        return ExtractionResult(
            extracted_bundle_path=final_bundle_dir,
            rows_extracted_total=int(manifest.get("metrics", {}).get("total_rows", 0) or 0),
        )

    try:
        bundle_layout.ensure_bundle_directory(tmp_bundle_dir)

        snapshot_date = _parse_snapshot_date(raw_path, spec)
        ingested_at = datetime.now(timezone.utc)

        system_cols: dict[str, Any] = {
            SYSTEM_COL_SOURCE_FILE: raw_path.name,
            SYSTEM_COL_INGESTED_AT: ingested_at,
            SYSTEM_COL_DATA_SNAPSHOT_DATE: snapshot_date,
        }

        artifact_relpath = "data.parquet"
        parquet_path = tmp_bundle_dir / artifact_relpath

        extractor = FastCSVExtractor(spec)
        total_rows = extractor.convert_to_parquet(
            file_path=raw_path,
            output_path=parquet_path,
            system_cols=system_cols,
        )

        manifest: dict[str, Any] = {
            "source_name": spec.name,
            "raw_file": str(raw_path),
            "status": "COMPLETED",
            "metrics": {"total_rows": int(total_rows)},
            "artifacts": [{"relpath": artifact_relpath, "type": "data", "count": int(total_rows)}],
        }
        bundle_layout.write_bundle_manifest(tmp_bundle_dir, manifest)

        # Atomic promote tmp -> final
        bundle_layout.finalize_tmp_bundle(tmp_bundle_dir=tmp_bundle_dir, final_bundle_dir=final_bundle_dir)

        return ExtractionResult(extracted_bundle_path=final_bundle_dir, rows_extracted_total=int(total_rows))

    except Exception:
        logger.exception("Extraction failed for %s", raw_path)
        try:
            bundle_layout.delete_bundle_dir(tmp_bundle_dir)
        except Exception:
            pass
        raise
