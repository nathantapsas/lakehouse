from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
import shutil
import time
import uuid
import logging
from pathlib import Path
from typing import Any, Iterable

from src.ingestion.domain import FileKey

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BundleLayout:
    """Filesystem layout for extraction bundles.

    Layout:
      staging_root/
        _tmp/    -> Working area for active extractions
        _trash/  -> Atomic deletion holding area (Trash Pattern)
        <source_name>/
          <bundle_hash>_<spec_prefix>/ -> Completed bundles waiting for load
    """

    staging_root: Path
    tmp_dirname: str = "_tmp"
    trash_dirname: str = "_trash"

    # ----------------------------
    # Paths
    # ----------------------------
    @property
    def tmp_root(self) -> Path:
        return self.staging_root / self.tmp_dirname

    @property
    def trash_root(self) -> Path:
        return self.staging_root / self.trash_dirname

    def get_bundle_directory_for(self, key: FileKey) -> Path:
        bundle_id = hashlib.md5(key.raw_file_metadata_signature.encode("utf-8")).hexdigest()[:16]
        spec_prefix = key.spec_hash[:12]
        return self.staging_root / key.source_name / f"{bundle_id}_{spec_prefix}"

    def get_tmp_bundle_directory_for(self, key: FileKey, *, claim_token: str) -> Path:
        bundle_id = hashlib.md5(key.raw_file_metadata_signature.encode("utf-8")).hexdigest()[:16]
        spec_prefix = key.spec_hash[:12]
        return self.tmp_root / key.source_name / claim_token / f"{bundle_id}_{spec_prefix}"

    def get_bundle_manifest_path(self, bundle_dir: Path) -> Path:
        return bundle_dir / "bundle.json"

    # ----------------------------
    # IO helpers
    # ----------------------------
    def ensure_bundle_directory(self, bundle_dir: Path) -> None:
        bundle_dir.mkdir(parents=True, exist_ok=True)

    def write_bundle_manifest(self, bundle_dir: Path, payload: dict[str, Any]) -> None:
        """Writes bundle.json atomically: tmp -> rename."""
        self.ensure_bundle_directory(bundle_dir)
        manifest_path = self.get_bundle_manifest_path(bundle_dir)
        tmp_path = manifest_path.with_suffix(".json.tmp")
        tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        os.replace(tmp_path, manifest_path)

    def is_complete_bundle_dir(self, bundle_dir: Path) -> bool:
        return self.get_bundle_manifest_path(bundle_dir).exists()

    def finalize_tmp_bundle(self, *, tmp_bundle_dir: Path, final_bundle_dir: Path) -> None:
        """
        Atomically promote a tmp bundle using a robust move strategy.
        Handles Windows micro-locks (Defender/OneDrive) with a tight retry loop.
        """
        self.ensure_bundle_directory(final_bundle_dir.parent)

        # 1) Trash Pattern: move existing directory out of the way
        if final_bundle_dir.exists():
            self.ensure_bundle_directory(self.trash_root)
            trash_path = self.trash_root / f"{final_bundle_dir.name}_{uuid.uuid4().hex}"
            try:
                os.rename(final_bundle_dir, trash_path)
            except OSError:
                shutil.rmtree(final_bundle_dir, ignore_errors=True)

        # 2) Promotion: move tmp -> final with retries for transient locks
        max_retries = 10
        for i in range(max_retries):
            try:
                os.replace(tmp_bundle_dir, final_bundle_dir)
                # After successful promote, prune now-empty tmp parents
                self._prune_empty_parents(tmp_bundle_dir.parent)
                return
            except PermissionError:
                if i == max_retries - 1:
                    raise
                time.sleep(0.05 * (i + 1))

    def delete_bundle_dir(self, bundle_dir: Path) -> None:
        """
        Delete a bundle directory, then prune empty parent directories up to staging_root.
        This prevents stale staging/<source>/ folders from accumulating.
        """
        if bundle_dir.exists():
            shutil.rmtree(bundle_dir, ignore_errors=True)
        # Try pruning parents (safe even if bundle_dir didn't exist)
        self._prune_empty_parents(bundle_dir.parent)

    # ----------------------------
    # Cleanup helpers
    # ----------------------------
    def _is_protected_dir(self, p: Path) -> bool:
        # Never delete staging_root itself, or special system dirs.
        return (p == self.staging_root) or (p.name.startswith("_"))

    def _prune_empty_parents(self, start_dir: Path) -> int:
        """
        Walk upward deleting empty dirs until staging_root or a protected dir is reached.
        Returns number of dirs removed.
        """
        removed = 0
        current = start_dir

        while True:
            if not current.exists() or not current.is_dir():
                break
            if self._is_protected_dir(current):
                break
            # stop if above staging_root
            try:
                current.relative_to(self.staging_root)
            except ValueError:
                break

            try:
                if any(current.iterdir()):
                    break
                current.rmdir()
                removed += 1
            except OSError:
                break

            current = current.parent

        return removed

    # ----------------------------
    # Cleanup / GC
    # ----------------------------
    def iter_final_bundles(self) -> Iterable[Path]:
        if not self.staging_root.exists():
            return []
        for source_dir in self.staging_root.iterdir():
            if not source_dir.is_dir():
                continue
            if source_dir.name.startswith("_"):
                continue
            for bundle_dir in source_dir.iterdir():
                if bundle_dir.is_dir():
                    yield bundle_dir

    def cleanup_trash(self) -> int:
        """Empties the _trash folder."""
        deleted = 0
        if self.trash_root.exists():
            try:
                shutil.rmtree(self.trash_root)
                self.trash_root.mkdir(parents=True, exist_ok=True)
                deleted = 1
            except Exception:
                for p in self.trash_root.iterdir():
                    try:
                        if p.is_dir():
                            shutil.rmtree(p, ignore_errors=True)
                        else:
                            p.unlink(missing_ok=True)
                        deleted += 1
                    except Exception:
                        pass
        return deleted

    def prune_empty_source_directories(self) -> int:
        """Removes source directories (e.g. staging/account) if they are empty."""
        pruned = 0
        if not self.staging_root.exists():
            return 0

        for source_dir in self.staging_root.iterdir():
            if not source_dir.is_dir():
                continue
            if source_dir.name.startswith("_"):
                continue

            try:
                if not any(source_dir.iterdir()):
                    source_dir.rmdir()
                    pruned += 1
            except OSError:
                pass
        return pruned

    def prune_tmp_run_dirs(self, run_id: str) -> int:
        """
        Remove empty _tmp/<source>/<run_id>/ directories left behind after promotions.
        (We do NOT wipe all of _tmp in case something else is running.)
        """
        removed = 0
        root = self.tmp_root
        if not root.exists():
            return 0

        for source_dir in root.iterdir():
            if not source_dir.is_dir():
                continue
            run_dir = source_dir / run_id
            if not run_dir.exists():
                continue

            # Remove if empty (or prune empties within it first)
            removed += self._prune_empty_parents(run_dir)

            # If run_dir still exists but is empty, remove it
            try:
                if run_dir.exists() and not any(run_dir.iterdir()):
                    run_dir.rmdir()
                    removed += 1
            except OSError:
                pass

            # Also prune empty source_dir under _tmp
            removed += self._prune_empty_parents(source_dir)

        return removed

    def cleanup_tmp_and_incomplete_bundles(self) -> int:
        """
        1. Wipes the entire _tmp directory (aggressive cleanup).
        2. Removes incomplete final bundles.
        """
        deleted = 0

        if self.tmp_root.exists():
            try:
                shutil.rmtree(self.tmp_root)
                deleted += 1
            except Exception as e:
                logger.warning(f"Failed to wipe tmp root: {e}")

        for final_bundle in list(self.iter_final_bundles()):
            if not self.is_complete_bundle_dir(final_bundle):
                self.delete_bundle_dir(final_bundle)
                deleted += 1

        return deleted

    def cleanup_after_commit(self, *, run_id: str) -> None:
        """
        Lightweight cleanup called after a successful commit.
        Keeps staging tidy without nuking tmp for other runs/processes.
        """
        self.prune_tmp_run_dirs(run_id)
        self.cleanup_trash()
        self.prune_empty_source_directories()
