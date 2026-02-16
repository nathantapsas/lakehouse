import logging
import re
from contextlib import contextmanager
from datetime import datetime, timezone

import duckdb

from ingestion.domain import DiscoveredFile, ExtractionResult, FileKey

logger = logging.getLogger(__name__)


def utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


class IngestionLedgerStore:
    """
    Minimal ingestion ledger backed by DuckDB + DuckLake.

    DB is a CHECKPOINT, not a queue:
      - records successfully loaded files
      - records file -> target tables lineage
      - orchestrator owns in-flight state in memory

    Tables:
      <catalog>.ops.loaded_files
      <catalog>.ops.loaded_file_targets
    """

    def __init__(self, *, duckdb_path: str, ducklake_attach_sql: str, auto_bootstrap: bool = True):
        self._duckdb_path = duckdb_path
        self._ducklake_attach_sql = ducklake_attach_sql
        self._auto_bootstrap = auto_bootstrap

        self._connection: duckdb.DuckDBPyConnection | None = None
        self._catalog: str | None = None

    def __enter__(self) -> "IngestionLedgerStore":
        if self._connection is not None:
            raise RuntimeError("Store connection already open")

        conn = duckdb.connect(self._duckdb_path)
        conn.execute(self._ducklake_attach_sql)

        self._catalog = self._parse_catalog_name(self._ducklake_attach_sql)
        self._connection = conn

        if self._auto_bootstrap:
            self._bootstrap()

        logger.debug("Ledger connected. catalog=%s duckdb=%s", self._catalog, self._duckdb_path)
        return self

    def __exit__(self, exc_type, exc_value, tb) -> None:
        if self._connection is None:
            return
        try:
            self._connection.close()
        finally:
            self._connection = None

    def _require_connection(self) -> duckdb.DuckDBPyConnection:
        if self._connection is None:
            raise RuntimeError("Store is not connected; use it as a context manager")
        return self._connection

    # ----------------------------
    # Public API
    # ----------------------------
    def start_run(
        self,
        *,
        run_id: str,
        expected_sources: set[str],
        discovered_sources: set[str],
        missing_files_by_date: dict[str, list[str]],
        duplicate_files_by_date: dict[str, list[str]],
    ) -> None:
        conn = self._require_connection()
        now = utc_now_naive()

        conn.execute(
            f"""
            INSERT INTO {self._ops('ingestion_runs')}
            (run_id, started_at_utc, status, expected_sources_json, found_sources_json, missing_sources_json,
             number_of_files_discovered)

            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                run_id,
                now,
                "RUNNING",
                expected_sources,
                discovered_sources,
                missing_files_by_date,
                len(discovered_sources),
            ],
        )
    def finalize_run(
        self,
        *,
        run_id: str,
        status: str,
        found_sources: set[str],
        missing_sources: set[str],
        number_of_processed_files: int,
        number_of_files_committed: int,
        error_message: str | None = None,
    ) -> None:
        conn = self._require_connection()
        now = utc_now_naive()

        conn.execute(
            f"""
            UPDATE {self._ops('ingestion_runs')}
            SET finished_at_utc = ?, status = ?, error_message = ?, number_of_files_processed = ?, number_of_files_committed = ?
            WHERE run_id = ?
            """,
            [
                now,
                status,
                error_message,
                number_of_processed_files,
                number_of_files_committed,
                run_id,
            ],
        )

    def get_completed_file_keys(self) -> set[FileKey]:
        conn = self._require_connection()
        rows = conn.execute(
            f"SELECT source_name, raw_file_metadata_signature, spec_hash FROM {self._ops('loaded_files')};"
        ).fetchall()

        return {FileKey(source_name=r[0], raw_file_metadata_signature=r[1], spec_hash=r[2]) for r in rows}

    def commit_batch(
        self,
        batch_results: list[tuple[DiscoveredFile, ExtractionResult]],
        load_plans: dict[str, list[str]],
        *,
        run_id: str,
        file_targets: dict[FileKey, set[str]],
    ) -> None:
        """
        Atomic: bulk load parquet data + record checkpoints + record lineage.

        load_plans:
          { "<catalog>.<schema>.<table>" -> ["/abs/path/a.parquet", "/abs/path/b.parquet"] }

        file_targets:
          { FileKey -> {"<catalog>.<schema>.<table>", ...} }
        """
        if not batch_results:
            return

        now = utc_now_naive()
        conn = self._require_connection()

        with self.transaction(conn) as tx:
            # 1) Ensure targets exist + bulk insert
            for table_fqn, files in load_plans.items():
                if not files:
                    continue
                self._ensure_target_table_from_parquet(tx, table_fqn, files)
                tx.execute(
                    f"INSERT INTO {table_fqn} BY NAME "
                    f"SELECT * FROM read_parquet(?, union_by_name=true)",
                    [files],
                )

            # 2) Write checkpoints (success only)
            ledger_rows = [
                (
                    d.file_key.source_name,
                    d.file_key.raw_file_metadata_signature,
                    d.file_key.spec_hash,
                    str(d.raw_file_path),
                    r.data_snapshot_date,
                    int(r.rows_extracted_total),
                    str(run_id),
                    now,
                )
                for (d, r) in batch_results
            ]

            tx.executemany(
                f"""
                INSERT INTO {self._ops('loaded_files')}
                (source_name, raw_file_metadata_signature, spec_hash, raw_file_path, data_snapshot_date, rows_loaded, run_id, loaded_at_utc)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ledger_rows,
            )

            # 3) Write lineage: file -> target tables
            lineage_rows: list[tuple[str, str, str, str, str, datetime]] = []
            for (d, r) in batch_results:
                targets = file_targets.get(d.file_key, set())
                for t in sorted(targets):
                    lineage_rows.append(
                        (
                            d.file_key.source_name,
                            d.file_key.raw_file_metadata_signature,
                            d.file_key.spec_hash,
                            t,
                            str(run_id),
                            now,
                        )
                    )

            if lineage_rows:
                tx.executemany(
                    f"""
                    INSERT INTO {self._ops('loaded_file_targets')}
                    (source_name, raw_file_metadata_signature, spec_hash, target_table_fqn, run_id, loaded_at_utc)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    lineage_rows,
                )

    # ----------------------------
    # Bootstrap / helpers
    # ----------------------------
    def _bootstrap(self) -> None:
        conn = self._require_connection()
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self._catalog}.ops")

        # conn.execute(
        #     f"""
        #     CREATE TABLE IF NOT EXISTS {self._ops('ingestion_runs')} (
        #         run_id                      VARCHAR PRIMARY KEY,
        #         started_at_utc              TIMESTAMP NOT NULL,
        #         finished_at_utc             TIMESTAMP,
        #         status                      VARCHAR NOT NULL,
        #         expected_sources_json       JSON NOT NULL,
        #         found_sources_json          JSON NOT NULL,
        #         missing_sources_json        JSON NOT NULL,

        #         number_of_files_discovered  INT NOT NULL DEFAULT 0,
        #         number_of_files_processed   INT NOT NULL DEFAULT 0,
        #         number_of_files_committed   INT NOT NULL DEFAULT 0,
        #         error_message               VARCHAR
        #     )

        #     """
        # )

        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._ops('loaded_files')} (
              source_name                 VARCHAR NOT NULL,
              raw_file_metadata_signature VARCHAR NOT NULL,
              spec_hash                   VARCHAR NOT NULL,
              raw_file_path               VARCHAR NOT NULL,
              data_snapshot_date          DATE,
              rows_loaded                 BIGINT  NOT NULL,
              run_id                      VARCHAR NOT NULL,
              loaded_at_utc               TIMESTAMP NOT NULL
            );
            """
        )

        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._ops('loaded_file_targets')} (
              source_name                 VARCHAR NOT NULL,
              raw_file_metadata_signature VARCHAR NOT NULL,
              spec_hash                   VARCHAR NOT NULL,
              target_table_fqn            VARCHAR NOT NULL,
              run_id                      VARCHAR NOT NULL,
              loaded_at_utc               TIMESTAMP NOT NULL
            );
            """
        )

    @staticmethod
    def _parse_catalog_name(attach_sql: str) -> str:
        match = re.search(r"\bAS\s+([A-Za-z_][A-Za-z0-9_]*)\b", attach_sql, flags=re.IGNORECASE)
        if not match:
            raise ValueError(
                "Could not parse catalog name from ducklake_attach_sql. "
                "Expected: ATTACH 'ducklake:...' AS <catalog_name>;"
            )
        return match.group(1)

    def _ops(self, table: str) -> str:
        if not self._catalog:
            raise RuntimeError("Catalog not set (store not connected?)")
        return f"{self._catalog}.ops.{table}"

    @contextmanager
    def transaction(self, conn: duckdb.DuckDBPyConnection):
        conn.execute("BEGIN TRANSACTION")
        try:
            yield conn
        except Exception:
            conn.execute("ROLLBACK")
            raise
        else:
            conn.execute("COMMIT")

    def _ensure_target_table_from_parquet(
        self,
        conn: duckdb.DuckDBPyConnection,
        target_table_fqn: str,
        parquet_files: list[str],
    ) -> None:
        """
        Ensure table exists and evolves via ADD COLUMN only.

        Strategy:
          - If table missing: CREATE TABLE AS SELECT ... LIMIT 0 (infers schema)
          - If exists: compare schemas, add missing columns as nullable
          - If type mismatch: raise (do not silently coerce)
        """
        parts = target_table_fqn.split(".")
        if len(parts) != 3:
            raise ValueError(f"Expected <catalog>.<schema>.<table>, got: {target_table_fqn}")
        catalog, schema, table = parts

        # Ensure schema exists
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

        # Check if table exists
        exists = conn.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
            LIMIT 1;
            """,
            [catalog, schema, table],
        ).fetchone()

        if not exists:
            # Create from parquet schema
            conn.execute(
                f"""
                CREATE TABLE {target_table_fqn} AS
                SELECT * FROM read_parquet(?, union_by_name=true) LIMIT 0;
                """,
                [parquet_files],
            )
            return

        # --- Table exists: evolve schema (add columns only) ---

        # Current table schema
        table_cols = conn.execute(f"DESCRIBE {target_table_fqn}").fetchall()
        # DESCRIBE returns: (column_name, column_type, null, key, default, extra)
        table_types: dict[str, str] = {r[0]: str(r[1]).upper() for r in table_cols}

        # Incoming parquet schema (use a relation so DuckDB resolves types)
        # We ask DuckDB for column types using DESCRIBE on a SELECT.
        incoming = conn.execute(
            "DESCRIBE SELECT * FROM read_parquet(?, union_by_name=true) LIMIT 0",
            [parquet_files],
        ).fetchall()
        incoming_types: dict[str, str] = {r[0]: str(r[1]).upper() for r in incoming}

        # Add missing columns
        for col, typ in incoming_types.items():
            if col not in table_types:
                # Add as nullable (DuckDB columns are nullable by default unless NOT NULL specified)
                conn.execute(f'ALTER TABLE {target_table_fqn} ADD COLUMN "{col}" {typ}')
                continue

            # Type mismatch? Fail fast.
            # (You can soften this with a whitelist of “compatible” widenings later.)
            if table_types[col] != typ:
                raise ValueError(
                    f"Schema mismatch for {target_table_fqn}.{col}: "
                    f"table has {table_types[col]} but parquet has {typ}. "
                    f"Refusing to auto-coerce."
                )