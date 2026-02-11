from __future__ import annotations

import fnmatch
import logging
import os
import time
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol, Sequence

from ingestion.bundle_layout import BundleLayout
from ingestion.csv_config import CSVIngestSpec
from ingestion.domain import DiscoveredFile, ExtractionResult, FileKey, LoadTarget, RunContext
from ingestion.extraction_task import execute_extraction_task
from ingestion.ledger_store import IngestionLedgerStore
from ingestion.utils import calculate_spec_hash

logger = logging.getLogger(__name__)


class LoadPlanner(Protocol):
    def plan_load(self, file_key: FileKey, extracted_bundle_path: Path) -> Sequence[LoadTarget]:
        ...


@dataclass(frozen=True)
class OrchestrationConfig:
    max_parallel_extractions: int = 14
    batch_loading_size: int = 50
    batch_loading_max_latency_seconds: float = 30.0
    idle_sleep_seconds: float = 0.05


class Orchestrator:
    """
    Coordinates: discover -> extract -> commit.

    DB is a checkpoint:
      - loaded_files
      - loaded_file_targets (lineage)

    All other state is in-memory for the duration of the run.
    """

    def __init__(
        self,
        *,
        store: IngestionLedgerStore,
        bundle_layout: BundleLayout,
        specs: Sequence[CSVIngestSpec],
        load_planner: LoadPlanner,
        config: OrchestrationConfig,
        data_dir: Path,
    ):
        self.store = store
        self.bundle_layout = bundle_layout
        self.specs = {s.name: s for s in specs}
        self.load_planner = load_planner
        self.config = config
        self.data_dir = data_dir

    def run(self, ctx: RunContext) -> None:
        with self.store as store, ProcessPoolExecutor(max_workers=self.config.max_parallel_extractions) as executor:
            completed_keys = store.get_completed_file_keys()

            all_files = self._discover_files()
            pending = [f for f in all_files if f.file_key not in completed_keys]

            logger.info("Discovery found %s files. %s require processing.", len(all_files), len(pending))

            in_flight: dict[Future[ExtractionResult], DiscoveredFile] = {}
            buffer: list[tuple[DiscoveredFile, ExtractionResult]] = []
            last_commit_time = time.time()

            try:
                while pending or in_flight:
                    # Dispatch
                    while len(in_flight) < self.config.max_parallel_extractions and pending:
                        task = pending.pop(0)
                        spec = self.specs[task.file_key.source_name]

                        tmp_dir = self.bundle_layout.get_tmp_bundle_directory_for(task.file_key, claim_token=ctx.run_id)
                        final_dir = self.bundle_layout.get_bundle_directory_for(task.file_key)

                        fut = executor.submit(
                            execute_extraction_task,
                            spec,
                            task,
                            tmp_dir,
                            final_dir,
                            self.bundle_layout,
                        )
                        in_flight[fut] = task

                    # Collect
                    if in_flight:
                        done, _ = wait(in_flight.keys(), timeout=0.1, return_when=FIRST_COMPLETED)
                        for fut in done:
                            task = in_flight.pop(fut)
                            try:
                                res = fut.result()
                                buffer.append((task, res))
                            except Exception:
                                logger.exception("Extraction failed for %s", task.raw_file_path)

                    # Commit hysteresis
                    buffer_size = len(buffer)
                    time_since_last = time.time() - last_commit_time

                    is_full = buffer_size >= self.config.batch_loading_size
                    is_stale = buffer_size > 0 and time_since_last >= self.config.batch_loading_max_latency_seconds
                    is_done = buffer_size > 0 and not pending and not in_flight

                    if is_full or is_stale or is_done:
                        self._commit_batch(store, ctx, buffer)
                        buffer.clear()
                        last_commit_time = time.time()

                    if not pending and not in_flight:
                        break

                    time.sleep(self.config.idle_sleep_seconds)
            finally:
                # End-of-run tidy-up (safe even if nothing committed)
                self.bundle_layout.cleanup_after_commit(run_id=ctx.run_id)

            logger.info("Ingestion run complete.")

    def _commit_batch(
        self,
        store: IngestionLedgerStore,
        ctx: RunContext,
        buffer: list[tuple[DiscoveredFile, ExtractionResult]],
    ) -> None:
        if not buffer:
            return

        logger.info("Committing batch of %s files...", len(buffer))

        load_plans: dict[str, list[str]] = {}
        file_targets: dict[FileKey, set[str]] = {}

        # Plan loads + lineage
        for discovered, res in buffer:
            targets = self.load_planner.plan_load(discovered.file_key, res.extracted_bundle_path)

            for t in targets:
                load_plans.setdefault(t.target_table_fqn, []).append(
                    str(res.extracted_bundle_path / t.artifact_relpath)
                )
                file_targets.setdefault(discovered.file_key, set()).add(t.target_table_fqn)

        # Atomic commit (data + checkpoints + lineage)
        store.commit_batch(buffer, load_plans, run_id=ctx.run_id, file_targets=file_targets)

        # Post-commit cleanup
        for _, res in buffer:
            self.bundle_layout.delete_bundle_dir(res.extracted_bundle_path)

        # Also prune any tmp run dirs + trash + empty sources
        self.bundle_layout.cleanup_after_commit(run_id=ctx.run_id)

    def _discover_files(self) -> list[DiscoveredFile]:
        discovered: list[DiscoveredFile] = []

        with os.scandir(self.data_dir) as it:
            for entry in it:
                if not entry.is_file():
                    continue

                for spec_name, spec in self.specs.items():
                    if not fnmatch.fnmatch(entry.name, spec.source.file_path_glob_pattern):
                        continue

                    stat = entry.stat()
                    mtime = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).replace(tzinfo=None)
                    metadata_signature = f"{entry.name}_{stat.st_size}_{stat.st_mtime}"

                    file_key = FileKey(
                        source_name=spec_name,
                        raw_file_metadata_signature=metadata_signature,
                        spec_hash=calculate_spec_hash(spec),
                    )

                    discovered.append(
                        DiscoveredFile(
                            file_key=file_key,
                            raw_file_path=Path(entry.path).absolute(),
                            raw_file_size_bytes=stat.st_size,
                            raw_file_mtime_utc=mtime,
                        )
                    )

        return discovered
