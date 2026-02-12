MODEL (
  name silver_dataphile.accounts_snapshot,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column @{sys_col_data_snapshot_date},
    batch_size 1,
  ),
  grain (account_number, @{sys_col_data_snapshot_date}),
  depends_on (silver_dataphile.clients_snapshot), -- Required for the foreign key audit
  audits (
    assert_foreign_key_same_day (
      parent_model := silver_dataphile.clients_snapshot,
      mappings := [(client_code, client_code)],
      child_time_column := @{sys_col_data_snapshot_date},
      parent_time_column := @{sys_col_data_snapshot_date},
      blocking := false
    )
  )
);

WITH transformed AS (
  SELECT
    @clean_account_number(account_number)                                                         AS account_number,
    @cast_to_integer(client_code)                                                                 AS client_code,
    @clean_status(status)                                                                         AS status,
    @cast_to_date(closed_date, format := '%m/%d/%y', prefer_past := True)                         AS closed_date,
    short_name::TEXT                                                                              AS short_name,
    ia_code::TEXT                                                                                 AS ia_code,
    account_type::TEXT                                                                            AS account_type,
    sub_type::TEXT                                                                                AS subtype,
    @clean_currency_code(currency)                                                                AS currency,
    portfolio_type_code::TEXT                                                                     AS portfolio_type_code,
    @cast_to_boolean(minimum_commission_check)                                                    AS minimum_commission_check,
    commission_type_code::TEXT                                                                    AS commission_type_code,
    credit_interest_code::TEXT                                                                    AS credit_interest_code,
    debit_interest_code::TEXT                                                                     AS debit_interest_code,
    @cast_to_boolean(is_discretionary)                                                            AS is_discretionary,

    -- TODO: option approval can sometimes be null, is null equivalent to 0 in this case?
    @cast_to_integer(option_approval_level_code)                                                  AS option_approval_level_code,

    @cast_to_boolean(is_allowed_to_trade)                                                         AS is_allowed_to_trade,
    class_code::TEXT                                                                              AS class_code,
    residence_code::TEXT                                                                          AS residence_code,

    @cast_to_numeric(settlement_date_cash_balance, 'positive_suffix' := 'CR')                     AS settlement_date_cash_balance,
    @cast_to_numeric(trade_date_cash_balance, 'positive_suffix' := 'CR')                          AS trade_date_cash_balance,
    @cast_to_numeric(market_value_amount, 'negative_suffix' := 'CR')                              AS market_value_amount,
    @cast_to_numeric(equity_amount, 'negative_prefix' := 'CR')                                    AS equity_amount,
    @cast_to_numeric(loan_value_amount, 'negative_suffix' := 'CR')                                AS loan_value_amount,
    @cast_to_numeric(buying_power_amount, 'negative_suffix' := 'CR')                              AS buying_power_amount,

    @cast_to_numeric(consolidation_level)                                                         AS consolidation_level,
    @cast_to_date(crm2_inception_date, '%m/%d/%y', prefer_past := True)                           AS crm2_inception_date,
    @cast_to_date(inception_date_override, '%m/%d/%Y')                                            AS inception_date_override,

    use_spouses_birthdate::TEXT                                                                   AS use_spouses_birthdate,
    @cast_to_boolean(open_items)                                                                  AS open_items,

    @cast_to_date(last_activity_date, '%m/%d/%y', prefer_past := True)                            AS last_activity_date,
    @cast_to_date(last_interest_date, '%m/%d/%y', prefer_past := True)                            AS last_interest_date,
    @cast_to_date(last_roi_date, '%m/%d/%y', prefer_past := True)                                 AS last_roi_date,
    @cast_to_date(last_statement_date, '%m/%d/%y', prefer_past := True)                           AS last_statement_date,
    @cast_to_date(last_seg_date, '%m/%d/%y', prefer_past := True)                                 AS last_seg_date,
    @cast_to_date(last_trade_date, '%m/%d/%y', prefer_past := True)                               AS last_trade_date,

    @cast_to_date(opened_date, format := '%m/%d/%y', prefer_past := True)                           AS opened_date,
    closed_reason::TEXT                                                                           AS closed_reason,
    
    @cast_to_boolean(check_credit)                                                                AS check_credit,


    @{sys_col_ingested_at},
    @{sys_col_data_snapshot_date}
  FROM bronze.accounts
),

deduplicated AS (
  @deduplicate(
    transformed,
    partition_by := (account_number, @{sys_col_data_snapshot_date}),
    order_by := ["@{sys_col_ingested_at} DESC"]
  )
)
SELECT * FROM deduplicated;