from sqlmesh import macro
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
    value: str,
    *,
    on_null: str = "preserve",
    null_default: str | None = None,
) -> str:
    expr = f"CAST(UPPER(TRIM(REPLACE({value}, '-', ''))) AS VARCHAR)"
    return _apply_null_policy(value, expr, on_null=on_null, null_default=null_default, field_name="Account number")

@macro()
def clean_cusip(
    evaluator: MacroEvaluator,
    value: str,
    *,
    on_null: str = "preserve",
    null_default: str | None = None,
) -> str:
    expr = f"CAST(UPPER(TRIM({value})) AS VARCHAR)"
    return _apply_null_policy(value, expr, on_null=on_null, null_default=null_default, field_name="CUSIP")

@macro()
def clean_currency_code(
    evaluator: MacroEvaluator,
    value: str,
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
    value: str,
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
def cast_to_numeric(
    evaluator: MacroEvaluator,
    value: str,
    positive_prefix: str | None = None,
    positive_suffix: str | None = None,
    negative_prefix: str | None = None,
    negative_suffix: str | None = None,
    *,
    on_null: str = "preserve",
    null_default: str | None = None,
) -> str:
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
    value: str,
    format: str,
    prefer_past: bool = True,
    future_year_threshold: int = 5,
    *,
    on_null: str = "preserve",
    null_default: str | None = None,
) -> str:
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
    value: str,
    *,
    true_tokens: tuple[str, ...] = ("T", "TRUE", "Y", "YES"),
    false_tokens: tuple[str, ...] = ("F", "FALSE", "N", "NO"),
    null_tokens: tuple[str, ...] = ("", "NULL", "N/A", "NA"),
    on_null: str = "preserve",
    null_default: str | None = None,
) -> str:
    """
    - Invalid (non-null, non-token) values ALWAYS error.
    - NULL handling is controlled by `on_null` / `null_default`.
    - null_tokens are treated the same as NULL input per `on_null`.
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
    value: str,
    *,
    on_null: str = "preserve",
    null_default: str | None = None,
) -> str:
    cleaned_value = f"TRIM(REPLACE({value}, ',', ''))"
    expr = f"CAST({cleaned_value} AS INTEGER)"  # invalid inputs error via CAST
    return _apply_null_policy(value, expr, on_null=on_null, null_default=null_default, field_name="Integer value")
