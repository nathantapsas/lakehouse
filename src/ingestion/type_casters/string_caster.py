from typing import Literal
from ingestion.type_casters.base_caster import CasterBase, CasterPlan


class StringCaster(CasterBase):
    output_type: Literal["string"]

    def plan(self, column_alias: str) -> CasterPlan:
        return CasterPlan(
            partials={},
            value_expression=f"TRY_CAST({column_alias} AS VARCHAR)"
        )

