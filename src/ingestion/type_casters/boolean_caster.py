from typing import Literal
from pydantic import Field, model_validator

from ingestion.type_casters.base_caster import CasterBase, CasterPlan
from ingestion.type_casters.common import sql_quote


class BooleanCaster(CasterBase):
    output_type: Literal["boolean"]

    true_values: list[str] = Field(default_factory=lambda: ["TRUE", "T", "YES", "Y"])
    false_values: list[str] = Field(default_factory=lambda: ["FALSE", "F", "NO", "N"])
    null_policy: Literal["preserve", "false", "true"] = "preserve"

    @model_validator(mode="after")
    def no_overlap(self) -> "BooleanCaster":
        if self.true_values and self.false_values:
            overlap = set(v.upper() for v in self.true_values) & set(v.upper() for v in self.false_values)
            if overlap:
                raise ValueError(f"true_values and false_values overlap: {sorted(overlap)}")
        return self

    def plan(self, column_alias: str) -> CasterPlan:
        clean_alias = self.build_alias(column_alias, "clean")
        clean_expression = f"UPPER(TRIM({column_alias}))"

        true_values_sql_list = ", ".join(sql_quote(value.upper()) for value in self.true_values)
        false_values_sql_list = ", ".join(sql_quote(value.upper()) for value in self.false_values)

        if self.true_values and self.false_values:
            value_expression = f"CASE WHEN {clean_alias} IN ({true_values_sql_list}) THEN TRUE WHEN {clean_alias} IN ({false_values_sql_list}) THEN FALSE ELSE NULL END"

        elif self.true_values:
            value_expression = f"CASE WHEN {column_alias} IS NULL THEN NULL WHEN {clean_alias} IN ({true_values_sql_list}) THEN TRUE ELSE FALSE END"

        elif self.false_values:
            value_expression = f"CASE WHEN {column_alias} IS NULL THEN NULL WHEN {clean_alias} IN ({false_values_sql_list}) THEN FALSE ELSE TRUE END"

        else:
            value_expression = f"TRY_CAST(TRIM({column_alias}) AS BOOLEAN)"

        if self.null_policy == "false":
            value_expression = f"COALESCE(({value_expression}), FALSE)"

        elif self.null_policy == "true":
            value_expression = f"COALESCE(({value_expression}), TRUE)"

        else:  # "preserve"
            value_expression = f"({value_expression})"

        return CasterPlan(partials={clean_alias: clean_expression}, value_expression=value_expression)


