import csv
import io
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Generator

import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pyarrow.compute as pc

from ingestion.csv_config import CSVIngestSpec

logger = logging.getLogger(__name__)


class CSVExtractionError(Exception):
    pass


@dataclass(frozen=True)
class ExtractedRecord:
    """Legacy dataclass for backward compatibility."""
    payload: dict[str, Any]
    source_file: str
    line_number: int
    raw_header: str
    errors: list[dict[str, str | None]]


class GeneratorStream(io.RawIOBase):
    """
    Adapts a Python generator yielding bytes into a file-like object 
    that PyArrow's C++ engine can read from natively.
    """
    def __init__(self, generator: Generator[bytes, None, None]):
        self._gen = generator
        self._leftover = b""
        self._iterator_finished = False

    def readable(self) -> bool:
        return True

    def readinto(self, b: bytearray) -> int | None:
        size = len(b)
        if size == 0:
            return 0

        data_len = len(self._leftover)
        chunks = [self._leftover]
        
        while data_len < size and not self._iterator_finished:
            try:
                chunk = next(self._gen)
                data_len += len(chunk)
                chunks.append(chunk)
            except StopIteration:
                self._iterator_finished = True

        if data_len == 0:
            return 0

        all_data = b"".join(chunks)
        count = min(data_len, size)
        b[:count] = all_data[:count]
        self._leftover = all_data[count:]

        return count


class FastCSVExtractor:
    INTERNAL_DELIMITER_BYTE = b'\x1f'
    INTERNAL_DELIMITER_STR = '\x1f'

    def __init__(self, csv_ingest_spec: CSVIngestSpec) -> None:
        self.spec = csv_ingest_spec
        
        # Build an efficient lookup map: Raw CSV Header -> DB Column Name
        # We perform this once at initialization
        self._header_map: dict[str, str] = {}
        for db_col_name, col_spec in self.spec.columns.items():
            candidates = col_spec.csv_header if isinstance(col_spec.csv_header, list) else [col_spec.csv_header]
            for candidate in candidates:
                self._header_map[candidate] = db_col_name

    def _get_clean_header(self, file_path: Path) -> list[str]:
        """
        Reads the first line, cleans it, renames duplicates, 
        MAPS to DB Column Names, and VALIDATES requirements.
        """
        encoding = self.spec.source.encoding
        quote_char = self.spec.source.quote_char
        delimiter = self.spec.source.delimiter

        effective_delimiter = f"{quote_char}{delimiter}{quote_char}"

        with open(file_path, 'r', encoding=encoding, newline="") as f:
            header_line = f.readline().rstrip('\r\n')

        if not header_line:
            raise CSVExtractionError(f"CSV file is empty or missing header: {file_path}")

        if not (header_line.startswith(quote_char) and header_line.endswith(quote_char)):
            raise CSVExtractionError(f"Header must start and end with quote_char '{quote_char}'")

        # 1. Physical Cleanup
        clean_line = header_line[len(quote_char):-len(quote_char)]
        clean_line = clean_line.replace(effective_delimiter, self.INTERNAL_DELIMITER_STR)

        reader = csv.reader([clean_line], delimiter=self.INTERNAL_DELIMITER_STR, quoting=csv.QUOTE_NONE)
        try:
            raw_headers = next(reader)
        except StopIteration:
            raise CSVExtractionError("Failed to parse cleaned header line.")

        # 2. Handle Physical Duplicates (e.g. "ID", "ID" -> "ID", "ID.1")
        unique_phys_headers = self._rename_duplicate_column_headers(raw_headers)

        # 3. Map to DB Schema & Validate
        final_headers: list[str] = []
        found_db_cols: set[str] = set()

        for phys_header in unique_phys_headers:
            if phys_header in self._header_map:
                # We found a mapped column!
                db_name = self._header_map[phys_header]
                final_headers.append(db_name)
                found_db_cols.add(db_name)
            else:
                # Pass through unknown columns as-is (e.g. 'Other Phone.1')
                # Alternatively, you could prefix them like '_extra_Other Phone.1'
                final_headers.append(phys_header)

        # 4. Check Required Columns
        missing_required: list[str] = []
        for db_col, col_spec in self.spec.columns.items():
            if col_spec.required and db_col not in found_db_cols:
                missing_required.append(db_col)

        if missing_required:
            raise CSVExtractionError(
                f"Missing required columns in file {file_path.name}.\n"
                f"Missing DB Columns: {missing_required}\n"
                f"Found Headers (Mapped): {sorted(list(found_db_cols))}"
            )

        return final_headers

    @staticmethod
    def _rename_duplicate_column_headers(header: list[str]) -> list[str]:
        from collections import Counter
        counts: Counter[str] = Counter()
        new_header: list[str] = []
        for column in header:
            count = counts[column]
            counts[column] += 1
            if count > 0:
                new_column = f'{column}.{count}'
                new_header.append(new_column)
            else:
                new_header.append(column)
        return new_header

    def _stream_cleaned_lines(self, file_path: Path, expected_number_of_columns: int) -> Generator[bytes, None, None]:
        """
        Yields CLEANED lines as raw bytes (Latin-1 safe).
        """
        encoding = self.spec.source.encoding
        quote_char_b = self.spec.source.quote_char.encode(encoding)
        delimiter_b = self.spec.source.delimiter.encode(encoding)
        
        complex_delimiter_b = quote_char_b + delimiter_b + quote_char_b

        with open(file_path, 'rb') as f:
            _ = f.readline() # Skip header, we already processed it for mapping and validation

            buffer = b""

            def normalize(parts: list[bytes]) -> bytes:
                if parts and parts[0].startswith(quote_char_b):
                    parts[0] = parts[0][1:]
                if parts and parts[-1].endswith(quote_char_b):
                    parts[-1] = parts[-1][:-1]

                return self.INTERNAL_DELIMITER_BYTE.join(parts) + b'\n'
            
            for physical in f:
                # Strip line terminator before buffering, so it can't end up inside a field
                line = physical.rstrip(b'\r\n')

                buffer += line
                if not buffer:
                    buffer = b""
                    continue  # Skip empty lines
                
                parts = buffer.split(complex_delimiter_b)

                if len(parts) < expected_number_of_columns:
                    logger.debug(f"Line has fewer columns than header after splitting by complex delimiter, buffering for next line.\n"
                                 f"Line: {buffer}\n"
                                 f"Expected Columns: {expected_number_of_columns}, Found: {len(parts)}")
                    continue  # Line is not complete yet, read more

                if len(parts) == expected_number_of_columns:
                    yield normalize(parts)
                    buffer = b""
                    continue

                raise CSVExtractionError(
                    f"Line has more columns than header after splitting by complex delimiter.\n"
                    f"Line: {buffer}\n"
                    f"Expected Columns: {expected_number_of_columns}, Found: {len(parts)}"
                )
            # Optional: Flush trailing buffer if it ends cleanly
            candidate = buffer.rstrip(b'\r\n')
            if candidate:
                logger.debug(f"End of file reached, processing trailing buffer: {candidate}")  # Debug: Show trailing buffer processing
                parts = candidate.split(complex_delimiter_b)
                if len(parts) == expected_number_of_columns:
                    logger.debug(f"Trailing line has expected number of columns, yielding normalized line.\n"
                                 f"Line: {candidate}\n"
                                 f"Expected Columns: {expected_number_of_columns}, Found: {len(parts)}")
                    yield normalize(parts)
                elif len(parts) != 0:
                    raise CSVExtractionError(
                        f"Trailing line has more columns than header after splitting by complex delimiter.\n"
                        f"Line: {candidate}\n"
                        f"Expected Columns: {expected_number_of_columns}, Found: {len(parts)}"
                    )


    def convert_to_parquet(self, file_path: Path, output_path: Path, system_cols: dict[str, Any]) -> int:
        """
        Orchestrates the conversion using mapped headers.
        """
        # 1. Get Schema-Mapped Headers
        # This will fail fast if the file schema is invalid
        mapped_header_names = self._get_clean_header(file_path)

        # 2. Configure PyArrow
        read_options = pv.ReadOptions(
            use_threads=True,
            column_names=mapped_header_names, # Use the mapped names here!
            autogenerate_column_names=False,
            encoding=self.spec.source.encoding
        )

        parse_options = pv.ParseOptions(
            delimiter=self.INTERNAL_DELIMITER_STR,
            quote_char=False, 
            double_quote=False,
            newlines_in_values=False
        )

        # Force strict string typing for all columns
        column_types = {name: pa.string() for name in mapped_header_names}
        
        convert_options = pv.ConvertOptions(
            check_utf8=True,
            column_types=column_types,
            strings_can_be_null=True
        )

        try:
            gen = self._stream_cleaned_lines(file_path, expected_number_of_columns=len(mapped_header_names))
            stream_wrapper = GeneratorStream(gen)
            
            table = pv.read_csv(
                stream_wrapper,
                read_options=read_options,
                parse_options=parse_options,
                convert_options=convert_options
            )
            
            row_count = table.num_rows
            if row_count == 0:
                return 0

            # 3. Add System Columns (using standard arrays)
            for col_name, val in system_cols.items():
                if isinstance(val, str):
                    pa_val = pa.array([val] * row_count, type=pa.string())
                else:
                    pa_val = pa.array([val] * row_count)
                table = table.append_column(col_name, pa_val)

            # 4. Write Parquet
            pq.write_table(table, output_path, compression='snappy')

            return row_count

        except Exception as e:
            raise CSVExtractionError(f"FastCSVExtraction failed: {e}")