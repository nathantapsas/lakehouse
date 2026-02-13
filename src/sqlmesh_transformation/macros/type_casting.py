from sqlmesh import macro, SQL
from sqlmesh.core.macros import MacroEvaluator


def _apply_null_policy(
    value_sql: str,
    expr_sql: str,
    *,
    on_null: str,
    null_default: str | None,
    field_name: str,
) -> str:
    if on_null not in ("preserve", "default", "error"):
        raise ValueError('on_null must be one of: "preserve", "default", "error"')
    if on_null == "default" and null_default is None:
        raise ValueError('null_default must be provided when on_null="default"')

    if on_null == "preserve":
        null_branch = "NULL"
    elif on_null == "default":
        null_branch = null_default
    else:  # "error"
        null_branch = f"error('{field_name} cannot be NULL')"

    return f"""
        CASE
            WHEN {value_sql} IS NULL THEN {null_branch}
            ELSE {expr_sql}
        END
    """


@macro()
def clean_account_number(
    evaluator: MacroEvaluator,
    value: SQL,
    *,
    on_null: str = "preserve",
    null_default: str | None = None,
) -> str:
    expr = f"CAST(UPPER(TRIM(REPLACE({value}, '-', ''))) AS VARCHAR)"
    return _apply_null_policy(value, expr, on_null=on_null, null_default=null_default, field_name="Account number")

@macro()
def clean_cusip(
    evaluator: MacroEvaluator,
    value: SQL,
    *,
    on_null: str = "preserve",
    null_default: str | None = None,
) -> str:
    expr = f"CAST(UPPER(TRIM({value})) AS VARCHAR)"
    return _apply_null_policy(value, expr, on_null=on_null, null_default=null_default, field_name="CUSIP")

@macro()
def clean_currency_code(
    evaluator: MacroEvaluator,
    value: SQL,
    *,
    on_null: str = "preserve",
    null_default: str | None = None,
) -> str:

    # TODO: Figure out what currency code "F", "E", "D", "Y" and "A" are
    expr = f"""
        CASE
            WHEN UPPER(TRIM({value})) = 'F' THEN 'UNKNOWN'
            WHEN UPPER(TRIM({value})) = 'D' THEN 'UNKNOWN'
            WHEN UPPER(TRIM({value})) = 'Y' THEN 'JPY'
            WHEN UPPER(TRIM({value})) = 'A' THEN 'AUD'
            WHEN UPPER(TRIM({value})) = 'E' THEN 'EUR'
            WHEN UPPER(TRIM({value})) = 'C' THEN 'CAD'
            WHEN UPPER(TRIM({value})) = 'U' THEN 'USD'
            ELSE error('Unrecognized currency code: ' || {value})
        END
    """
    return _apply_null_policy(value, expr, on_null=on_null, null_default=null_default, field_name="Currency code")

@macro()
def clean_status(
    evaluator: MacroEvaluator,
    value: SQL,
    *,
    on_null: str = "preserve",
    null_default: str | None = None,
) -> str:
    expr = f"""
        CASE
            WHEN UPPER(TRIM({value})) = 'A' THEN 'ACTIVE'
            WHEN UPPER(TRIM({value})) = 'C' THEN 'CLOSED'
            WHEN UPPER(TRIM({value})) = 'P' THEN 'PROSPECTIVE'
            WHEN UPPER(TRIM({value})) = 'D' THEN 'DELETED'

            ELSE error('Unrecognized status value: ' || {value})
        END
    """
    return _apply_null_policy(value, expr, on_null=on_null, null_default=null_default, field_name="Status")

@macro()
def split_part(
    evaluator: MacroEvaluator, 
    value: SQL, 
    delimiter: str, 
    part_index: int,
    null_policy: str = "preserve",
    null_default: str | None = None,
) -> str:
    """
    Splits a string by a delimiter and returns the specified part (1-based index). The input is normalized by trimming whitespace.
    If the delimiter is not found in the value, an error is raised with a message indicating the missing delimiter and the original value. 
    If the part_index is out of range (e.g. requesting part 3 from a string that only has 2 parts), an error will be raised by the SPLIT_PART function.

    NULL handling is controlled by `null_policy` / `null_default`.
    null_policy:    
        - "preserve": NULL inputs result in NULL output
        - "default": NULL inputs result in `null_default` output (must be provided if null_policy="default")
        - "error": NULL inputs result in an error with a message indicating the field name that cannot be NULL.
    """
    expr = f"""
        CASE 
            WHEN STRPOS({value}, '{delimiter}') = 0 THEN ERROR('Delimiter "{delimiter}" not found in value: ' || {value})
            ELSE TRIM(SPLIT_PART({value}, '{delimiter}', {part_index}))
        END
    """
    return _apply_null_policy(value, expr, on_null=null_policy, null_default=null_default, field_name=f"Part {part_index} of {value}")


@macro()
def cast_to_numeric(
    evaluator: MacroEvaluator,
    value: SQL,
    positive_prefix: str | None = None,
    positive_suffix: str | None = None,
    negative_prefix: str | None = None,
    negative_suffix: str | None = None,
    *,
    on_null: str = "preserve",
    null_default: str | None = None,
) -> str:
    """
    Cleans and casts a value to numeric. The input is normalized by trimming whitespace and removing commas.
    The sign of the number can be determined by optional prefixes/suffixes. For example, if positive_prefix is '$' and negative_suffix is 'CR', then:
    - '$100' would be cast as 100
    - '100CR' would be cast as -100
    - '100' would be cast as -100 since it doesn't have the positive prefix or negative suffix.
    Invalid (non-null, non-numeric) values will error via the CAST.

    NULL handling is controlled by `on_null` / `null_default`.
    on_null:    
        - "preserve": NULL inputs result in NULL output
        - "default": NULL inputs result in `null_default` output (must be provided if on_null="default")
        - "error": NULL inputs result in an error with a message indicating the field name that cannot be NULL.
    """

    if sum(x is not None for x in [positive_prefix, positive_suffix, negative_prefix, negative_suffix]) > 1:
        raise ValueError(
            "Only one of positive_prefix, positive_suffix, negative_prefix, negative_suffix can be provided."
        )

    cleaned_value = f"TRIM(REPLACE({value}, ',', ''))"

    # Invalid non-null inputs should error via CAST failure (no fallback).
    if positive_prefix is not None:
        expr = f"""
            CASE
                WHEN {cleaned_value} LIKE '{positive_prefix}%'
                THEN CAST(SUBSTRING({cleaned_value}, {len(positive_prefix) + 1}) AS NUMERIC)
                ELSE CAST({cleaned_value} AS NUMERIC) * -1
            END
        """
    elif positive_suffix is not None:
        expr = f"""
            CASE
                WHEN {cleaned_value} LIKE '%{positive_suffix}'
                THEN CAST(SUBSTRING({cleaned_value}, 1, LENGTH({cleaned_value}) - {len(positive_suffix)}) AS NUMERIC)
                ELSE CAST({cleaned_value} AS NUMERIC) * -1
            END
        """
    elif negative_prefix is not None:
        expr = f"""
            CASE
                WHEN {cleaned_value} LIKE '{negative_prefix}%'
                THEN CAST(SUBSTRING({cleaned_value}, {len(negative_prefix) + 1}) AS NUMERIC) * -1
                ELSE CAST({cleaned_value} AS NUMERIC)
            END
        """
    elif negative_suffix is not None:
        expr = f"""
            CASE
                WHEN {cleaned_value} LIKE '%{negative_suffix}'
                THEN CAST(SUBSTRING({cleaned_value}, 1, LENGTH({cleaned_value}) - {len(negative_suffix)}) AS NUMERIC) * -1
                ELSE CAST({cleaned_value} AS NUMERIC)
            END
        """
    else:
        expr = f"CAST({cleaned_value} AS NUMERIC)"

    return _apply_null_policy(value, expr, on_null=on_null, null_default=null_default, field_name="Numeric value")


@macro()
def cast_to_date(
    evaluator: MacroEvaluator,
    value: SQL,
    format: str,
    prefer_past: bool = True,
    future_year_threshold: int = 5,
    *,
    on_null: str = "preserve",
    null_default: str | None = None,
) -> str:
    """
    Parses a date from a string using the provided format. The input is normalized by trimming whitespace.
    The `format` should be in the syntax expected by the underlying SQL engine's STRPTIME function (e.g. '%Y-%m-%d' for '2024-01-01').

    To handle 2-digit year formats, the `prefer_past` and `future_year_threshold` parameters control how parsed dates are adjusted:
        - If `prefer_past` is True, parsed dates that are more than `future_year_threshold` years in the future 
            (compared to current date) will be adjusted back by 100 years. 
            For example, if the current year is 2024 and `future_year_threshold` is 5, then a parsed date 
            of '30-DEC-25' (2025) would be adjusted to '30-DEC-1925' since 2025 is more than 5 years in the future. 
            However, '30-DEC-27' would not be adjusted since it is within the 5-year threshold.
        - If `prefer_past` is False, no adjustment is made and the parsed date is returned as-is.
    Invalid (non-null, non-date) values will error via the CAST.

    NULL handling is controlled by `on_null` / `null_default`.
    on_null:    
        - "preserve": NULL inputs result in NULL output
        - "default": NULL inputs result in `null_default` output (must be provided if on_null="default")
        - "error": NULL inputs result in an error with a message indicating the field name that cannot be NULL.
    """
    parsed_date = f"CAST(STRPTIME(TRIM({value}), '{format}') AS DATE)"
    boundary_date = f"CAST(CURRENT_DATE + INTERVAL '{future_year_threshold} years' AS DATE)"
    adjusted_date = f"CAST({parsed_date} - INTERVAL '100 years' AS DATE)"

    if not prefer_past:
        expr = parsed_date
    else:
        expr = f"""
            CASE
                WHEN {parsed_date} > {boundary_date} THEN {adjusted_date}
                ELSE {parsed_date}
            END
        """

    return _apply_null_policy(value, expr, on_null=on_null, null_default=null_default, field_name="Date value")


@macro()
def cast_to_boolean(
    evaluator: MacroEvaluator,
    value: SQL,
    *,
    true_tokens: tuple[str, ...] = ("T", "TRUE", "Y", "YES"),
    false_tokens: tuple[str, ...] = ("F", "FALSE", "N", "NO"),
    null_tokens: tuple[str, ...] = ("", "NULL", "N/A", "NA"),
    on_null: str = "preserve",
    null_default: str | None = None,
) -> str:
    """
    Converts a string to a boolean based on specified true/false/null tokens.
    The input is normalized by trimming whitespace, converting to uppercase, and casting to VARCHAR.
    Invalid (non-null, non-matching) values will error via the ELSE branch.

    NULL handling is controlled by `on_null` / `null_default`.
    on_null:    
    - "preserve": NULL inputs result in NULL output
    - "default": NULL inputs result in `null_default` output (must be provided if on_null="default")
    - "error": NULL inputs result in an error with a message indicating the field name that cannot be NULL.
    """
    norm = f"UPPER(TRIM(CAST({value} AS VARCHAR)))"

    true_list = ", ".join(f"'{t.upper()}'" for t in true_tokens)
    false_list = ", ".join(f"'{t.upper()}'" for t in false_tokens)
    null_list = ", ".join(f"'{t.upper()}'" for t in null_tokens)

    if on_null not in ("preserve", "default", "error"):
        raise ValueError('on_null must be one of: "preserve", "default", "error"')
    if on_null == "default" and null_default is None:
        raise ValueError('null_default must be provided when on_null="default"')

    if on_null == "preserve":
        null_token_branch = "NULL"
    elif on_null == "default":
        null_token_branch = null_default
    else:
        null_token_branch = "error('Boolean value cannot be NULL')"

    expr = f"""
        CASE
            WHEN {norm} IN ({null_list}) THEN {null_token_branch}
            WHEN {norm} IN ({true_list}) THEN TRUE
            WHEN {norm} IN ({false_list}) THEN FALSE
            ELSE error('Unrecognized boolean token: ' || {value})
        END
    """

    return _apply_null_policy(value, expr, on_null=on_null, null_default=null_default, field_name="Boolean value")


@macro()
def cast_to_integer(
    evaluator: MacroEvaluator,
    value: SQL,
    *,
    on_null: str = "preserve",
    null_default: str | None = None,
) -> str:
    """
    Cleans and casts a value to integer. The input is normalized by trimming whitespace and removing commas.
    Invalid (non-null, non-integer) values will error via the CAST. 

    NULL handling is controlled by `on_null` / `null_default`.
    on_null:    
    - "preserve": NULL inputs result in NULL output
    - "default": NULL inputs result in `null_default` output (must be provided if on_null="default")
    - "error": NULL inputs result in an error with a message indicating the field name that cannot be NULL.
    """
    cleaned_value = f"TRIM(REPLACE({value}, ',', ''))"
    expr = f"CAST({cleaned_value} AS INTEGER)"  # invalid inputs error via CAST
    return _apply_null_policy(value, expr, on_null=on_null, null_default=null_default, field_name="Integer value")
