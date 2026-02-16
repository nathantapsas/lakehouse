import logging
import cProfile
import functools
import io
import json
import hashlib
from pathlib import Path
import pstats
import re
from typing import Any, Callable
from datetime import datetime, timezone

from src.ingestion.csv_config import CSVIngestSpec

logger = logging.getLogger(__name__)

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def profile_to_log(func: Callable[[Any], Any]) -> Callable[[Any], Any]:
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        # only start if another profile isn't already running (e.g. in nested calls)

        if hasattr(wrapper, "_profiling_active") and wrapper._profiling_active:
            return func(*args, **kwargs)

        pr = cProfile.Profile()
        pr.enable()
        
        result = func(*args, **kwargs)
        
        pr.disable()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats(pstats.SortKey.CUMULATIVE)
        ps.print_stats(30)
        
        logger.debug(f"PROFILING [{func.__name__}]:\n{s.getvalue()}")
        return result
    return wrapper

def md5_record_hash(record: dict[str, Any]) -> str:
    json_str = json.dumps(record, sort_keys=True, separators=(',', ':'), default=str)
    return hashlib.md5(json_str.encode("utf-8")).hexdigest()


def sha256_file_hash(file_path: Path) -> str:
    hash_sha256 = hashlib.sha256()
    with file_path.open("rb") as f:
        for chunk in iter(lambda: f.read(131072), b""):
            hash_sha256.update(chunk)
    return hash_sha256.hexdigest()

def calculate_spec_hash(spec: CSVIngestSpec) -> str:
    """
    Hashes the dictionary representation of the config.
    Effectively: MD5(YAML Content - Comments - Whitespace)
    """
    # 1. Get the raw dictionary. 
    # 'exclude_defaults=True' strips out date.today() and anything else 
    # that wasn't explicitly written in your YAML file.
    data = spec.model_dump(exclude_defaults=True, mode='json')

    # 2. Dump to JSON with sorting.
    # We use a custom encoder to handle 'sets' if they exist in your config.
    json_str = json.dumps(
        data, 
        sort_keys=True,             # Sort dictionary keys (a:1, b:2)
        default=deterministic_serializer # Handle sets/dates
    )
    
    return hashlib.md5(json_str.encode("utf-8")).hexdigest()

def deterministic_serializer(obj: Any) -> Any:
    """
    Helper to serialize types that JSON doesn't handle natively,
    ensuring they are sorted deterministically.
    """
    if isinstance(obj, (set, frozenset)):
        # Convert set to list and sort it
        return sorted(list(obj), key=str)
    # Fallback for dates/other types (though mode='json' above handles most)
    return str(obj)


def parse_data_snapshot_date_from_filename(filename: str, filename_date_regex: str, filename_date_format: str) -> datetime | None:
    if not filename_date_regex or not filename_date_format:
        return None
    
    match = re.search(filename_date_regex, filename)
    if not match:
        logger.warning(f"Filename '{filename}' does not match the provided regex '{filename_date_regex}'. Cannot parse data snapshot date.")
        return None
    
    return datetime.strptime(match.group(1), filename_date_format)
