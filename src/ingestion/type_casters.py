from abc import ABC, abstractmethod
from typing import Any, Literal, Union
from typing_extensions import Annotated

from pydantic import BaseModel, ConfigDict, Field, model_validator


def _sql_str_lit(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def _sql_in_list(values: list[str] | tuple[str, ...]) -> str:
    return "(" + ", ".join(_sql_str_lit(v) for v in values) + ")"


def _struct_sql(raw_expr: str, cast_expr: str, err_expr: str) -> str:
    return f"struct_pack(raw := {raw_expr}, value := {cast_expr}, errors := {err_expr})"


class TypeCasterBase(BaseModel, ABC):
    model_config = ConfigDict(extra="forbid", strict=True)
    required: bool = True
    null_strings: set[str] = Field(default_factory=lambda: {"", "NULL", "N/A", "NA"})

    @abstractmethod
    def _cast(self, col: str) -> str:
        raise NotImplementedError("Subclasses must implement this method")

    def __call__(self, raw_value: str) -> Any:
        return self._cast(raw_value)


class StringCaster(TypeCasterBase):
    output_type: Literal["string"] = "string"
    length: int | None = None
    to_upper: bool = True
    to_lower: bool = False

    @model_validator(mode="after")
    def check_constraints(self):
        if self.to_upper and self.to_lower:
            raise ValueError("Cannot set both to_upper and to_lower to True")
        return self

    def _cast(self, col: str) -> str:
        raw_sql = f"CAST({col} AS VARCHAR)"
        null_tokens_sql = _sql_in_list(tuple(s.upper() for s in self.null_strings))
        empty_list = "CAST([] AS VARCHAR[])"

        transform_expr = "t"
        if self.to_upper:
            transform_expr = "UPPER(t)"
        elif self.to_lower:
            transform_expr = "LOWER(t)"

        if self.length is None:
            errors_sql = empty_list
            value_sql = "CASE WHEN c IS NULL THEN NULL ELSE v_raw END"
        else:
            n = int(self.length)
            value_sql = f"CASE WHEN c IS NULL THEN NULL WHEN LENGTH(v_raw) > {n} THEN NULL ELSE v_raw END"
            errors_sql = (
                f"CASE WHEN c IS NOT NULL AND LENGTH(v_raw) > {n} "
                f"THEN ['Column \"{col}\": String exceeds max length {n}: \"' || v_raw || '\"'] "
                f"ELSE {empty_list} END"
            )

        return f"""(
        SELECT struct_pack(raw := r, value := {value_sql}, errors := {errors_sql})
        FROM (
            SELECT r, c, {transform_expr} AS v_raw
            FROM (
                SELECT raw_text AS r, CASE WHEN UPPER(TRIM(raw_text)) IN {null_tokens_sql} THEN NULL ELSE TRIM(raw_text) END AS c, TRIM(raw_text) AS t
                FROM (SELECT CAST({col} AS VARCHAR) AS raw_text)
            )
        ))"""


class DateCaster(TypeCasterBase):
    output_type: Literal["date"] = "date"
    interpret_mode: Literal["pivot", "prefer_past", "prefer_future"] = "pivot"
    pivot_year_short: int | None = None
    reference_date_sql: str = "CURRENT_DATE"

    def _cast(self, col: str) -> str:
        pivot = int(self.pivot_year_short or 0)
        mode_lit = _sql_str_lit(self.interpret_mode)
        null_list = _sql_in_list(tuple(s.upper() for s in self.null_strings))
        empty_list = "CAST([] AS VARCHAR[])"

        year_logic = (
            f"CASE WHEN raw_year > 99 THEN raw_year "
            f"WHEN {mode_lit} = 'pivot' THEN CASE WHEN raw_year <= {pivot} THEN current_century + raw_year ELSE current_century - 100 + raw_year END "
            f"WHEN {mode_lit} = 'prefer_past' THEN CASE WHEN (2000 + raw_year) <= current_year THEN 2000 + raw_year ELSE 1900 + raw_year END "
            f"WHEN {mode_lit} = 'prefer_future' THEN CASE WHEN (2000 + raw_year) >= current_year THEN 2000 + raw_year ELSE 1900 + raw_year END "
            "ELSE NULL END"
        )

        errors_sql = f"CASE WHEN r IS NOT NULL AND c IS NOT NULL AND casted_val IS NULL THEN ['Column \"{col}\": Invalid date format: \"' || r || '\"'] ELSE {empty_list} END"

        return f"""(
        SELECT struct_pack(raw := r, value := casted_val, errors := {errors_sql})
        FROM (
            SELECT r, c, TRY_CAST(LPAD(y.year4::VARCHAR, 4, '0') || '-' || LPAD(month_i::VARCHAR, 2, '0') || '-' || LPAD(day_i::VARCHAR, 2, '0') AS DATE) AS casted_val
            FROM (
                SELECT *, {year_logic} AS year4 FROM (
                    SELECT base.*, TRY_CAST(parts.y_s AS INT) AS raw_year, TRY_CAST(parts.m_s AS INT) AS month_i, TRY_CAST(parts.d_s AS INT) AS day_i
                    FROM (
                        SELECT r, c, EXTRACT(YEAR FROM {self.reference_date_sql})::INT AS current_year, ((EXTRACT(YEAR FROM {self.reference_date_sql})::INT / 100) * 100)::INT AS current_century,
                               (c IS NOT NULL AND LENGTH(c) = 8 AND regexp_full_match(c, '^[0-9]{{8}}$')) AS is8,
                               (c IS NOT NULL AND LENGTH(c) = 6 AND regexp_full_match(c, '^[0-9]{{6}}$')) AS is6,
                               (c IS NOT NULL AND POSITION('-' IN c) > 0) AS has_d, (c IS NOT NULL AND POSITION('/' IN c) > 0) AS has_s
                        FROM (SELECT CAST({col} AS VARCHAR) AS r, CASE WHEN UPPER(TRIM(CAST({col} AS VARCHAR))) IN {null_list} THEN NULL ELSE TRIM(CAST({col} AS VARCHAR)) END AS c)
                    ) base
                    LEFT JOIN LATERAL (
                        SELECT 
                            CASE WHEN is8 THEN SUBSTR(c, 1, 4) WHEN is6 THEN SUBSTR(c, 1, 2) WHEN has_d THEN SUBSTR(c, STRPOS(c, '-') + 4, 4) WHEN has_s THEN SUBSTR(c, STRPOS(c, '/') + 4, 4) ELSE NULL END AS y_s,
                            CASE WHEN is8 THEN SUBSTR(c, 5, 2) WHEN is6 THEN SUBSTR(c, 3, 2) WHEN has_d THEN SPLIT_PART(c, '-', 1) WHEN has_s THEN SPLIT_PART(c, '/', 1) ELSE NULL END AS m_s,
                            CASE WHEN is8 THEN SUBSTR(c, 7, 2) WHEN is6 THEN SUBSTR(c, 5, 2) WHEN has_d THEN SPLIT_PART(c, '-', 2) WHEN has_s THEN SPLIT_PART(c, '/', 2) ELSE NULL END AS d_s
                    ) parts ON TRUE
                )
            ) y
        ))"""


class DecimalCaster(TypeCasterBase):
    output_type: Literal["decimal"] = "decimal"
    positive_prefix: str | None = None
    positive_suffix: str | None = None
    negative_prefix: str | None = None
    negative_suffix: str | None = None

    def _cast(self, col: str) -> str:
        null_tokens = _sql_in_list(tuple(s.upper() for s in self.null_strings))
        empty_list = "CAST([] AS VARCHAR[])"

        sgn_sql, stripped_sql = "1", "c"
        if self.negative_prefix:
            tok = _sql_str_lit(self.negative_prefix.upper())
            sgn_sql = f"CASE WHEN STARTS_WITH(UPPER(c), {tok}) THEN -1 ELSE 1 END"
            stripped_sql = f"CASE WHEN STARTS_WITH(UPPER(c), {tok}) THEN SUBSTR(c, {len(self.negative_prefix) + 1}) ELSE c END"
        elif self.negative_suffix:
            tok = _sql_str_lit(self.negative_suffix.upper())
            sgn_sql = f"CASE WHEN UPPER(RIGHT(c, {len(self.negative_suffix)})) = {tok} THEN -1 ELSE 1 END"
            stripped_sql = f"CASE WHEN UPPER(RIGHT(c, {len(self.negative_suffix)})) = {tok} THEN LEFT(c, LENGTH(c) - {len(self.negative_suffix)}) ELSE c END"
        elif self.positive_suffix:
            tok = _sql_str_lit(self.positive_suffix.upper())
            sgn_sql = f"CASE WHEN UPPER(RIGHT(c, {len(self.positive_suffix)})) = {tok} THEN 1 ELSE -1 END"
            stripped_sql = f"CASE WHEN UPPER(RIGHT(c, {len(self.positive_suffix)})) = {tok} THEN LEFT(c, LENGTH(c) - {len(self.positive_suffix)}) ELSE c END"
        elif self.positive_prefix:
            tok = _sql_str_lit(self.positive_prefix.upper())
            sgn_sql = f"CASE WHEN STARTS_WITH(UPPER(c), {tok}) THEN 1 ELSE -1 END"
            stripped_sql = f"CASE WHEN STARTS_WITH(UPPER(c), {tok}) THEN SUBSTR(c, {len(self.positive_prefix) + 1}) ELSE c END"

        errors_sql = f"CASE WHEN r IS NOT NULL AND c IS NOT NULL AND casted_val IS NULL THEN ['Column \"{col}\": Could not parse \"' || r || '\" as Decimal'] ELSE {empty_list} END"

        return f"""(
        SELECT struct_pack(raw := r, value := casted_val, errors := {errors_sql})
        FROM (
            SELECT r, c, ({sgn_sql})::DECIMAL * TRY_CAST(REPLACE({stripped_sql}, ',', '') AS DECIMAL) AS casted_val
            FROM (
                SELECT raw_text AS r, CASE WHEN UPPER(TRIM(raw_text)) IN {null_tokens} THEN NULL ELSE TRIM(raw_text) END AS c
                FROM (SELECT CAST({col} AS VARCHAR) AS raw_text)
            )
        ))"""


class BooleanCaster(TypeCasterBase):
    output_type: Literal["boolean"] = "boolean"
    true_values: list[str] = Field(default_factory=lambda: ["TRUE", "YES", "Y"])
    false_values: list[str] = Field(default_factory=lambda: ["FALSE", "NO", "N"])
    null_policy: Literal["preserve", "false", "true"] = "preserve"

    def _cast(self, col: str) -> str:
        tv_sql, fv_sql = _sql_in_list(self.true_values), _sql_in_list(self.false_values)
        null_tokens_sql = _sql_in_list(tuple(s.upper() for s in self.null_strings))
        empty_list = "CAST([] AS VARCHAR[])"
        null_val = {"preserve": "NULL", "false": "FALSE", "true": "TRUE"}[self.null_policy]

        errors_sql = f"CASE WHEN r IS NOT NULL AND c IS NOT NULL AND v IS NULL THEN ['Column \"{col}\": Invalid boolean: \"' || r || '\"'] ELSE {empty_list} END"

        return f"""(
        SELECT struct_pack(raw := r, value := v, errors := {errors_sql})
        FROM (
            SELECT r, c, CASE WHEN c IN {tv_sql} THEN TRUE WHEN c IN {fv_sql} THEN FALSE ELSE {null_val} END AS v
            FROM (
                SELECT CAST({col} AS VARCHAR) AS r, CASE WHEN UPPER(TRIM(CAST({col} AS VARCHAR))) IN {null_tokens_sql} THEN NULL ELSE UPPER(TRIM(CAST({col} AS VARCHAR))) END AS c
            )
        ))"""


class IntegerCaster(TypeCasterBase):
    output_type: Literal["integer"] = "integer"

    def _cast(self, col: str) -> str:
        null_tokens_sql = _sql_in_list(tuple(s.upper() for s in self.null_strings))
        empty_list = "CAST([] AS VARCHAR[])"
        errors_sql = f"CASE WHEN r IS NOT NULL AND c IS NOT NULL AND casted_val IS NULL THEN ['Column \"{col}\": Cannot parse integer from \"' || r || '\"'] ELSE {empty_list} END"

        return f"""(
        SELECT struct_pack(raw := r, value := casted_val, errors := {errors_sql})
        FROM (
            SELECT r, c, TRY_CAST(REPLACE(c, ',', '') AS BIGINT) AS casted_val
            FROM (
                SELECT raw_text AS r, CASE WHEN UPPER(TRIM(raw_text)) IN {null_tokens_sql} THEN NULL ELSE TRIM(raw_text) END AS c
                FROM (SELECT CAST({col} AS VARCHAR) AS raw_text)
            )
        ))"""


TypeCaster = Annotated[
    Union[StringCaster, DateCaster, DecimalCaster, BooleanCaster, IntegerCaster],
    Field(discriminator="output_type"),
]