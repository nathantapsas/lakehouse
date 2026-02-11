from typing import Literal
from ingestion.type_casters.base_caster import CasterBase, CasterPlan


class IntegerCaster(CasterBase):
    output_type: Literal["integer"]

    def plan(self, column_alias: str) -> CasterPlan:
        return CasterPlan(
            partials={},
            value_expression=f"TRY_CAST(REPLACE(TRIM({column_alias}), ',', '') AS INTEGER)"
        )
