from typing import Optional, Literal
from pydantic import model_validator

from ingestion.type_casters.base_caster import CasterBase, CasterPlan
from ingestion.type_casters.common import sql_quote


class DecimalCaster(CasterBase):
    output_type: Literal["decimal"]

    positive_suffix: Optional[str] = None
    negative_suffix: Optional[str] = None
    positive_prefix: Optional[str] = None
    negative_prefix: Optional[str] = None

    @model_validator(mode="after")
    def check_exclusivity(self) -> "DecimalCaster":
        if sum(v is not None for v in (self.positive_suffix, self.negative_suffix, self.positive_prefix, self.negative_prefix)) > 1:
            raise ValueError("Only one of positive_suffix, negative_suffix, positive_prefix, or negative_prefix can be provided.")
        return self

    def plan(self, column_alias: str) -> CasterPlan:
        # --- 1. Define aliases for intermediate and final expressions ---
        clean_alias = self.build_alias(column_alias, "clean")
        sign_alias = self.build_alias(column_alias, "sign")
        unsigned_str_alias = self.build_alias(column_alias, "unsigned_str")

        # --- 2. Define the base cleaning expression. This is the only expensive part. ---
        clean_expression = f"UPPER(REPLACE(TRIM({column_alias}), ',', ''))"

        # --- 3. Dynamically build the sign-handling logic ---
        #    This avoids generating `CASE WHEN FALSE THEN ...`
        negative_conditions: list[str] = []
        if self.negative_suffix:
            negative_conditions.append(f"ENDS_WITH({clean_alias}, {sql_quote(self.negative_suffix.upper())})")
        if self.negative_prefix:
            negative_conditions.append(f"STARTS_WITH({clean_alias}, {sql_quote(self.negative_prefix.upper())})")

        if negative_conditions:
            sign_expression = f"CASE WHEN {' OR '.join(negative_conditions)} THEN -1 ELSE 1 END"
        else:
            # If no negative affixes are defined, the sign is always positive.
            sign_expression = "1"

        # --- 4. Dynamically build the unsigned string logic ---
        #    This generates a clean CASE statement with only the necessary WHEN clauses.
        when_clauses: list[str] = []
        if self.negative_suffix:
            when_clauses.append(f"WHEN ENDS_WITH({clean_alias}, {sql_quote(self.negative_suffix.upper())}) THEN LEFT({clean_alias}, LENGTH({clean_alias}) - {len(self.negative_suffix)})")
        if self.positive_suffix:
            when_clauses.append(f"WHEN ENDS_WITH({clean_alias}, {sql_quote(self.positive_suffix.upper())}) THEN LEFT({clean_alias}, LENGTH({clean_alias}) - {len(self.positive_suffix)})")
        if self.negative_prefix:
            when_clauses.append(f"WHEN STARTS_WITH({clean_alias}, {sql_quote(self.negative_prefix.upper())}) THEN SUBSTR({clean_alias}, {len(self.negative_prefix) + 1})")
        if self.positive_prefix:
            when_clauses.append(f"WHEN STARTS_WITH({clean_alias}, {sql_quote(self.positive_prefix.upper())}) THEN SUBSTR({clean_alias}, {len(self.positive_prefix) + 1})")

        if when_clauses:
            unsigned_str_expression = f"CASE {' '.join(when_clauses)} ELSE {clean_alias} END"
        else:
            # If no affixes are defined, the unsigned string is the same as the cleaned string.
            unsigned_str_expression = clean_alias
            
        # --- 5. The final value expression references the aliases from the `base` CTE ---
        value_expression = f"(TRY_CAST({unsigned_str_alias} AS DECIMAL) * {sign_alias})"

        return CasterPlan(
            partials={
                clean_alias: clean_expression,
                sign_alias: sign_expression,
                unsigned_str_alias: unsigned_str_expression,
            },
            value_expression=value_expression
        )

