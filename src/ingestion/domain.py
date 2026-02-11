from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class FileKey:
    """
    Identity for an ingested raw file under a specific ingestion spec.

    raw_file_metadata_signature:
      A cheap content proxy (often filename + size + mtime). If you want stronger
      guarantees, replace it with a cryptographic digest.

    spec_hash:
      Included so changing the spec forces reprocessing of the same raw file.
    """
    source_name: str
    raw_file_metadata_signature: str
    spec_hash: str


@dataclass(frozen=True)
class DiscoveredFile:
    """A raw file discovered on disk."""
    file_key: FileKey
    raw_file_path: Path
    raw_file_size_bytes: int
    raw_file_mtime_utc: datetime  # naive UTC


@dataclass(frozen=True)
class RunContext:
    """Per-run context."""
    run_id: str


@dataclass(frozen=True)
class ExtractionResult:
    """Result returned by an extraction worker."""
    extracted_bundle_path: Path
    rows_extracted_total: int


@dataclass(frozen=True)
class LoadTarget:
    """
    A planned load of one artifact into one target table.

    The orchestrator groups artifacts by target_table_fqn and the store performs
    bulk inserts using read_parquet(..., union_by_name=true).
    """
    target_table_fqn: str
    artifact_relpath: str
    insert_sql: str
    bind_params: tuple[Any, ...] = ()
