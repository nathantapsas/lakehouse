import csv
import logging
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Set

logger = logging.getLogger(__name__)

class BusinessCalendar:
    """
    Handles business day calculations based on a specific Holiday CSV format.
    
    Rule: A day is a holiday (non-business day) ONLY if:
          1. It is a Weekend (Saturday/Sunday)
          2. OR both 'USA' and 'CAN' regions have a holiday of Type 'B'.
    """

    def __init__(self, holiday_file_path: Path | None):
        self._holidays: Set[date] = set()
        if holiday_file_path and holiday_file_path.exists():
            self._load_holidays(holiday_file_path)
        elif holiday_file_path:
            logger.warning(f"Holiday file configured but not found at: {holiday_file_path}. Assuming no holidays.")

    def _load_holidays(self, path: Path) -> None:
        """
        Parses the vendor CSV. 
        Format expected: "Holiday" (MM/DD/YY), "Region Code" (USA/CAN), "Type" (B/N/T)
        """
        # Intermediate storage: date -> set of regions that are closed on this date
        closures: dict[date, set[str]] = defaultdict(set)

        try:
            with open(path, 'r', encoding='utf-8', errors='replace') as f:
                # The file format has double quotes around headers and values based on the example
                reader = csv.DictReader(f, delimiter='\t', quotechar='"')
                
                # Normalize headers (strip whitespace/quotes if manual parsing was needed, 
                # but csv.DictReader usually handles standard quoted CSVs well).
                # Adjusting based on provided example: headers are "Holiday", "Region Code", "Type"
                
                for row in reader:
                    # Defensive key access
                    date_str = row.get("Holiday")
                    region = row.get("Region Code")
                    type_code = row.get("Type")

                    if not date_str or not region or not type_code:
                        continue

                    # Filter based on business logic
                    if region not in ('USA', 'CAN'):
                        continue
                    if type_code != 'B':
                        continue

                    try:
                        # Parse "01/01/07" -> MM/DD/YY
                        dt = datetime.strptime(date_str, "%m/%d/%y").date()
                        closures[dt].add(region)
                    except ValueError:
                        logger.warning(f"Could not parse holiday date: {date_str}")
                        continue

            # Finalize logic: Date is a holiday only if BOTH markets are closed
            for dt, regions in closures.items():
                if 'USA' in regions and 'CAN' in regions:
                    self._holidays.add(dt)
            
            logger.info(f"Loaded business calendar. Found {len(self._holidays)} global market holidays.")

        except Exception as e:
            logger.error(f"Failed to load holiday file {path}: {e}")
            raise

    def is_business_day(self, d: date) -> bool:
        # 1. Check Weekend (5=Sat, 6=Sun)
        if d.weekday() >= 5:
            return False
        
        # 2. Check Global Holiday
        if d in self._holidays:
            return False
            
        return True

    def get_previous_business_day(self, reference_date: date) -> date:
        """
        Returns the closest previous date that is a business day.
        """
        candidate = reference_date - timedelta(days=1)
        
        # Loop backwards until we find a business day
        # (Safety break after 30 days to prevent infinite loops in bad config)
        attempts = 0
        while not self.is_business_day(candidate):
            candidate -= timedelta(days=1)
            attempts += 1
            if attempts > 30:
                logger.error(f"Could not find a business day preceding {reference_date}. Calendar configuration may be broken.")
                return reference_date - timedelta(days=1) # Fallback

        return candidate