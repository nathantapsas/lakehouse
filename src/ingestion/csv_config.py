# ETL/etl/csv_config.py
from glob import glob
import logging
from csv import QUOTE_ALL
import os
import re
from typing import Annotated, Literal, Self, Union
import yaml


from pydantic import BaseModel, ConfigDict, model_validator, Field


# from ingestion.type_casters import TypeCaster, StringCaster
from ingestion.type_casters.boolean_caster import BooleanCaster
from ingestion.type_casters.date_caster import DateCaster
from ingestion.type_casters.decimal_caster import DecimalCaster
from ingestion.type_casters.integer_caster import IntegerCaster
from ingestion.type_casters.string_caster import StringCaster

logger = logging.getLogger(__name__)

 
TypeCasterSpec = Annotated[
    Union[BooleanCaster, DecimalCaster, DateCaster, IntegerCaster, StringCaster],
    Field(discriminator="output_type"),
]


class StrictBaseModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class SourceSpec(StrictBaseModel):
    file_path_glob_pattern: str
    delimiter: str
    quoting: Literal[1, 2, 3, 4, 5]
    quote_char: str
    encoding: str

    filename_date_regex: str | None = r"^.*_(\d{6})\d*\.txt$"
    filename_date_format: str | None = "%y%m%d"

    @model_validator(mode="after")
    def validate_spec(self) -> Self:
        if self.quoting == QUOTE_ALL and not self.quote_char:
            raise ValueError("quote_char must be specified when quoting is set to QUOTE_ALL")

        if (self.filename_date_format is None) ^ (self.filename_date_regex is None):
            raise ValueError("Both filename_date_format and filename_date_regex must be specified together")

        return self

class ForeignKeySpec(StrictBaseModel):
    local_columns: list[str]
    remote_table: str
    remote_columns: list[str]


class ColumnSpec(StrictBaseModel):
    csv_header: str | list[str]
    description: str | None = None
    type_caster: TypeCasterSpec = Field(default_factory=lambda: StringCaster(output_type="string"))
    is_nullable: bool = False
    required: bool = True


class CSVIngestSpec(StrictBaseModel):
    name: str

    source: SourceSpec
    
    write_disposition: Literal["merge", "append", "replace"] = "merge"
    loading_strategy: Literal["incremental", "snapshot"] = "incremental"

    business_key: list[str] = Field(default_factory=lambda: list())

    columns: dict[str, ColumnSpec]  # db_column_name -> ColumnSpec

    foreign_keys: list[ForeignKeySpec] = Field(default_factory=lambda: list())

    @model_validator(mode="after")
    def validate_spec(self) -> Self:
        db_column_names = list(self.columns.keys())
        if duplicates := {name for name in db_column_names if db_column_names.count(name) > 1}:
            raise ValueError(f"Duplicate column names found in column_specs: {duplicates}")

        for column_name in self.columns.keys():
            if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", column_name):
                raise ValueError(f"Invalid column name '{column_name}'. Must start with a letter or underscore, "
                                 "followed by letters, digits, or underscores.")
        
        for business_key_column in self.business_key:
            if business_key_column not in self.columns:
                raise ValueError(f"Business key column '{business_key_column}' is not defined in columns")
        
        for fk_spec in self.foreign_keys:
            for local_column in fk_spec.local_columns:
                if local_column not in self.columns:
                    raise ValueError(f"Foreign key local column '{local_column}' is not defined in columns")

        return self

    @property
    def source_headers(self) -> set[str]:
        headers: set[str] = set()
        for spec in self.columns.values():
            if isinstance(spec.csv_header, list):
                headers.update(spec.csv_header)
            else:
                headers.add(spec.csv_header)
        return headers

    def get_schema(self) -> dict[str, str]:
        schema: dict[str, str] = {}
        for db_column_name, column_spec in self.columns.items():
            schema[db_column_name] = column_spec.type_caster.output_type
        return schema


def load_csv_source_configs_from_directory(directory_path: str) -> list[CSVIngestSpec]:
    file_paths = sorted(glob(os.path.join(directory_path, "*.yaml")))

    configs: dict[str, CSVIngestSpec] = {}
    for file_path in file_paths:
        with open(file_path, "r") as file:
            config_yaml = yaml.safe_load(file)
            try:
                config = CSVIngestSpec.model_validate(config_yaml)

                if config.name in configs:
                    raise ValueError(f"Duplicate CSV source config name '{config.name}' found in file: {file_path}")

                configs[config.name] = config

            except Exception as e:
                raise ValueError(f"Error loading CSV source config from {file_path}: {e}")
    
    if not configs:
        logger.warning(f"No CSV source configuration files found in directory: {directory_path}")

    return list(configs.values())