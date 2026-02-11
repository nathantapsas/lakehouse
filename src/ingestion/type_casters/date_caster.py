from datetime import date
from typing import Literal, Optional

from pydantic import model_validator

from ingestion.type_casters.base_caster import CasterBase, CasterPlan


def _iso_from_parts_str(year_sql: str, month_sql: str, day_sql: str) -> str:
    year = f"CAST({year_sql} AS VARCHAR)"
    month = f"LPAD(CAST({month_sql} AS VARCHAR), 2, '0')"
    day = f"LPAD(CAST({day_sql} AS VARCHAR), 2, '0')"
    # return f"CONCAT({year}, '-', {month}, '-', {day})"
    return f"{year} || '-' || {month} || '-' || {day}"


def _yy_to_yyyy_sql_str(yy_sql: str, current_year: int, mode: str, pivot: int | None) -> str:
    y2 = f"CAST({yy_sql} AS INT)"
    century = (current_year // 100) * 100

    if mode == "pivot":
        if pivot is None:
            raise ValueError("Pivot year must be provided for pivot mode.")

        return f"CASE WHEN {y2} <= {pivot} THEN {century} + {y2} ELSE {century - 100} + {y2} END"

    if mode == "prefer_past":
        return f"CASE WHEN {century} + {y2} <= {current_year} THEN {century} + {y2} ELSE {century - 100} + {y2} END"

    if mode == "prefer_future":
        return f"CASE WHEN {century} + {y2} >= {current_year} THEN {century} + {y2} ELSE {century + 100} + {y2} END"

    raise ValueError(f"Invalid interpret_mode '{mode}'")


class DateCaster(CasterBase):
    output_type: Literal["date"]
    interpret_mode: Literal["prefer_past", "prefer_future", "pivot"] = "pivot"
    pivot_year_short: Optional[int] = 25

    @model_validator(mode="after")
    def check_pivot_required(self) -> "DateCaster":
        if self.interpret_mode == "pivot" and self.pivot_year_short is None:
            raise ValueError("pivot_year_short must be provided for interpret_mode='pivot'")
        return self

    def plan(self, column_alias: str) -> CasterPlan:
        current_year = date.today().year
        digits_alias = self.build_alias(column_alias, "digits")
        dlen_alias = self.build_alias(column_alias, "digits_length")
        yyyy1_alias = self.build_alias(column_alias, "yyyy1")
        yyyy2_alias = self.build_alias(column_alias, "yyyy2")

        # Define the base digit-stripping expression
        digits_expression = f"REGEXP_REPLACE(TRIM({column_alias}), '[^0-9]', '', 'g')"

        # Build self-contained expressions for the `base` CTE
        partials = {
            digits_alias: digits_expression,
            dlen_alias: f"LENGTH({digits_alias})",
            yyyy1_alias: _yy_to_yyyy_sql_str(
                f"SUBSTR({digits_alias}, 1, 2)", current_year, self.interpret_mode, self.pivot_year_short
            ),
            yyyy2_alias: _yy_to_yyyy_sql_str(
                f"SUBSTR({digits_alias}, 5, 2)", current_year, self.interpret_mode, self.pivot_year_short
            ),
        }

        # --- Expressions for the `typed` CTE can now safely use the aliases from `base` ---
        eight_ymd_iso = _iso_from_parts_str(
            f"SUBSTR({digits_alias}, 1, 4)", f"SUBSTR({digits_alias}, 5, 2)", f"SUBSTR({digits_alias}, 7, 2)"
        )
        eight_mdy_iso = _iso_from_parts_str(
            f"SUBSTR({digits_alias}, 5, 4)", f"SUBSTR({digits_alias}, 1, 2)", f"SUBSTR({digits_alias}, 3, 2)"
        )
        eight_digit_expr = f"CASE WHEN {dlen_alias} = 8 THEN COALESCE(TRY_CAST({eight_ymd_iso} AS DATE), TRY_CAST({eight_mdy_iso} AS DATE)) END"

        six_ymd_iso = _iso_from_parts_str(yyyy1_alias, f"SUBSTR({digits_alias}, 3, 2)", f"SUBSTR({digits_alias}, 5, 2)")
        six_mdy_iso = _iso_from_parts_str(yyyy2_alias, f"SUBSTR({digits_alias}, 1, 2)", f"SUBSTR({digits_alias}, 3, 2)")
        six_digit_expr = f"CASE WHEN {dlen_alias} = 6 THEN COALESCE(TRY_CAST({six_ymd_iso} AS DATE), TRY_CAST({six_mdy_iso} AS DATE)) END"

        literal_try_expr = f"TRY_CAST(TRIM({column_alias}) AS DATE)"

        value_expression = f"COALESCE({eight_digit_expr}, {six_digit_expr}, {literal_try_expr})"

        return CasterPlan(partials=partials, value_expression=value_expression)
