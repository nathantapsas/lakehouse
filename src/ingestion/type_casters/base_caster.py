from dataclasses import dataclass
from abc import ABC, abstractmethod
from pydantic import BaseModel, ConfigDict, Field


@dataclass(frozen=True)
class CasterPlan:
    # alias -> SQL string expression
    partials: dict[str, str]
    # sql string expression for the "final typed value" of this column, which is allowed to reference partials by alias
    value_expression: str


class CasterBase(BaseModel, ABC):
    output_type: str
    model_config = ConfigDict(extra="forbid", strict=True)
    null_strings: set[str] = Field(default_factory=lambda: {""})

    @abstractmethod
    def plan(self, column_alias: str) -> CasterPlan: ...

    def build_alias(self, column_alias: str, step: str) -> str:
        return f"{column_alias}__{step}"

    def get_nullify_sql(self, column_name: str) -> str:
        """Standardized nullification: returns NULL if token matches, else TRIM(val)"""
        tokens = ", ".join(f"'{s.upper()}'" for s in self.null_strings)
        return f"""
            CASE 
                WHEN UPPER(TRIM("{column_name}")) IN ({tokens}) THEN NULL 
                ELSE TRIM("{column_name}") 
            END
        """