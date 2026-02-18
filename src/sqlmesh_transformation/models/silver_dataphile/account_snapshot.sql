MODEL (
  name silver_dataphile.accounts_snapshot,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column __data_as_of_date,
  ),
  grain (account_number, __data_as_of_date),
  depends_on (silver_dataphile.clients_snapshot), -- Required for the foreign key audit
  audits (
    assert_foreign_key_same_day (
      parent_model := silver_dataphile.clients_snapshot,
      mappings := [(client_code, client_code)],
      child_time_column := __data_as_of_date,
      parent_time_column := __data_as_of_date,
      blocking := false
    )
  ),
);

WITH date_map AS (
  SELECT
    @{sys_col_data_snapshot_date},
    data_as_of_date                                                                               AS __data_as_of_date
  FROM bronze_meta.dataphile_asof_map
    WHERE data_as_of_date BETWEEN @start_ds AND @end_ds
),

transformed AS (
  SELECT
    @clean_account_number(account_number)                                                         AS account_number,
    @cast_to_integer(client_code)                                                                 AS client_code,

    ia_code::TEXT                                                                                 AS ia_code,
    short_name::TEXT                                                                              AS short_name,
    @clean_status(status)                                                                         AS status,

    @cast_to_integer(@split_part(account_type, '-', 1))                                           AS account_type_code,
    CASE
      WHEN account_type_code = 0 THEN 'Cash'
      WHEN account_type_code = 1 THEN 'COD'
      WHEN account_type_code = 2 THEN 'Margin-Long'
      WHEN account_type_code = 3 THEN 'Margin-Short'
      WHEN account_type_code = 4 THEN 'Client-Name'
      WHEN account_type_code = 9 THEN 'Prospect'
      ELSE ERROR(printf('Unexpected account type code: %s', account_type_code))
    END                                                                                           AS account_type,

    -- @split_part(account_type, '-', 2)                                                             AS account_type,
    @split_part(sub_type, '-', 1)                                                                 AS sub_type_code,
    CASE
      WHEN sub_type IS NULL THEN 'Cash'
      WHEN sub_type NOT LIKE '%-%' THEN ERROR(printf('Unexpected sub type format: %s', sub_type))
      ELSE @split_part(sub_type, '-', 2)
    END::TEXT                                                                                     AS sub_type,

    portfolio_type_code::TEXT                                                                     AS portfolio_type_code,
    CASE
      WHEN portfolio_type_code IS NULL THEN 'Commission'
      WHEN portfolio_type_code = 'M' THEN 'Managed'
      WHEN portfolio_type_code = 'S' THEN 'SMA'
      WHEN portfolio_type_code = 'F' THEN 'Fee-Based'
      WHEN portfolio_type_code = 'O' THEN 'Other'
      ELSE ERROR(printf('Unexpected portfolio type code: %s', portfolio_type_code))
    END                                                                                           AS portfolio_type,
    @cast_to_boolean(minimum_commission_check)                                                    AS minimum_commission_check,
    commission_type_code::TEXT                                                                    AS commission_type_code,
    credit_interest_code::TEXT                                                                    AS credit_interest_code,
    debit_interest_code::TEXT                                                                     AS debit_interest_code,
    @cast_to_boolean(is_discretionary)                                                            AS is_discretionary,

    -- TODO: option approval can sometimes be null, is null equivalent to 0 in this case?
    @cast_to_integer(option_approval_level_code, on_null := "default", null_default := "0")       AS option_approval_level_code,

    @cast_to_integer(consolidation_level)                                                         AS consolidation_level,
    @cast_to_boolean(is_allowed_to_trade)                                                         AS is_allowed_to_trade,
    class_code::TEXT                                                                              AS class_code,
    @split_part(residence_code, '-', 1)::TEXT                                                     AS residence_code,
    @split_part(residence_code, '-', 2)::TEXT                                                     AS residence,


    @clean_currency_code(currency)                                                                AS currency,
    -- Numeric Fields
    @cast_to_numeric(settlement_date_cash_balance, 'positive_suffix' := 'CR')                     AS settlement_date_cash_balance,
    @cast_to_numeric(trade_date_cash_balance, 'positive_suffix' := 'CR')                          AS trade_date_cash_balance,
    @cast_to_numeric(market_value_amount, 'negative_suffix' := 'CR')                              AS market_value_amount,
    @cast_to_numeric(equity_amount, 'negative_prefix' := 'CR')                                    AS equity_amount,
    @cast_to_numeric(loan_value_amount, 'negative_suffix' := 'CR')                                AS loan_value_amount,
    @cast_to_numeric(buying_power_amount, 'negative_suffix' := 'CR')                              AS buying_power_amount,

    -- Date Fields
    @cast_to_date(opened_date, format := '%m/%d/%y', prefer_past := True)                         AS opened_date,
    @cast_to_date(closed_date, format := '%m/%d/%y', prefer_past := True)                         AS closed_date,

    @cast_to_date(inception_date_override, '%m/%d/%Y')                                            AS inception_date_override,
    @cast_to_date(crm2_inception_date, '%m/%d/%y', prefer_past := True)                           AS crm2_inception_date,

    @cast_to_date(last_activity_date, '%m/%d/%y', prefer_past := True)                            AS last_activity_date,
    @cast_to_date(last_interest_date, '%m/%d/%y', prefer_past := True)                            AS last_interest_date,
    @cast_to_date(last_roi_date, '%m/%d/%y', prefer_past := True)                                 AS last_roi_date,
    @cast_to_date(last_statement_date, '%m/%d/%y', prefer_past := True)                           AS last_statement_date,
    @cast_to_date(last_seg_date, '%m/%d/%y', prefer_past := True)                                 AS last_seg_date,
    @cast_to_date(last_trade_date, '%m/%d/%y', prefer_past := True)                               AS last_trade_date,


    use_spouses_birthdate::TEXT                                                                   AS use_spouses_birthdate,
    @cast_to_boolean(open_items)                                                                  AS open_items,

    -- closed_reason_code::TEXT                                                                      AS closed_reason_code,
    closed_reason::TEXT                                                                           AS closed_reason,
    
    @cast_to_boolean(check_credit)                                                                AS check_credit,


    @{sys_col_ingested_at},
    -- @{sys_col_source_file},
    __data_as_of_date

  FROM bronze.accounts a
  JOIN date_map m
    ON a.@{sys_col_data_snapshot_date} = m.@{sys_col_data_snapshot_date}
),

deduplicated AS (
  @deduplicate(
    transformed,
    partition_by := (account_number, __data_as_of_date ),
    order_by := ["@{sys_col_ingested_at} DESC"]
  )
)

SELECT * FROM deduplicated;