
def sql_quote(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"

def sql_identifier_quote(identifier: str) -> str:
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def remove_sql_identifier_quotes(identifier: str) -> str:
    if identifier.startswith('"') and identifier.endswith('"'):
        unescaped = identifier[1:-1].replace('""', '"')
        return unescaped
    return identifier


def build_error_struct_sql(
    *,
    source_column_name: str,   # SQL expr returning the source column name (text)
    target_type: str | None,
    error_reason: str,
    source_value_sql: str,    # SQL expr returning the problematic value
    params_json_sql: str      # SQL expr returning JSON w/ extra context
) -> str:
    """
    Returns a SQL snippet that yields a STRUCT with the same shape as what
    wrap_as_struct() currently produces in CasterBase.
    """

    target_type_sql = (
        f"CAST('{target_type}' AS VARCHAR)"
        if target_type is not None
        else "CAST(NULL AS VARCHAR)"
    )

    return f"""
    STRUCT_PACK(
        source_column    := {sql_quote(source_column_name)},
        target_type      := {target_type_sql},
        error_reason     := {sql_quote(error_reason)},
        source_value_vc  := CAST({source_value_sql} AS VARCHAR),
        params_json      := CAST('{params_json_sql}' AS JSON)
    )
    """

def build_null_error_struct_sql() -> str:
    return """CAST(NULL AS STRUCT(
        source_column     VARCHAR,
        target_type     VARCHAR,
        error_reason    VARCHAR,
        source_value_vc VARCHAR,
        params_json     JSON
    ))"""

def wrap_value_and_error_struct(value_sql: str, error_sql: str) -> str:
    """
    Takes a SQL expression for a value and a SQL expression for an error struct
    and wraps them into a final STRUCT: {value, error}.
    """
    return f"""
        STRUCT_PACK(
            value := {value_sql},
            error := ({error_sql})
        )
    """

def build_ok_result_struct_sql(value_sql: str) -> str:
    return f"STRUCT_PACK(value := {value_sql}, error := {build_null_error_struct_sql()})"

def build_error_result_struct_sql(value_sql: str, error_sql: str) -> str:
    return f"STRUCT_PACK(value := {value_sql}, error := {error_sql})"