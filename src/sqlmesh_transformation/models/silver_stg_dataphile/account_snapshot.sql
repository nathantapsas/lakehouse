MODEL (
  name silver_stg_dataphile.account_snapshot,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column __data_snapshot_date,
    batch_size 1,
  ),
  grain (account_number, __data_snapshot_date),
  depends_on (silver_stg_dataphile.client_snapshot), -- Required for the foreign key audit
  audits (
    assert_foreign_key_same_day (
      parent_model := silver_stg_dataphile.client_snapshot,
      mappings := [(client_code, client_code)],
      child_time_column := '__data_snapshot_date',
      parent_time_column := '__data_snapshot_date'
    )
  )
);

WITH transformed AS (
  SELECT
    @clean_account_number(account_number)                                                         AS account_number,
    status::TEXT                                                                                  AS status,
    @cast_to_date(close_date, format := '%m/%d/%y', prefer_past := True)                          AS close_date,
    short_name::TEXT                                                                              AS short_name,
    ia_code::TEXT                                                                                 AS ia_code,
    account_type::TEXT                                                                            AS account_type,
    sub_type::TEXT                                                                                AS subtype,
    @clean_currency_code(currency)                                                                AS currency,
    portfolio_type::TEXT                                                                          AS portfolio_type,
    @cast_to_boolean(minimum_commission_check)                                                    AS minimum_commission_check,
    commission_type::TEXT                                                                         AS commission_type,
    credit_int::TEXT                                                                              AS credit_int,
    debit_int::TEXT                                                                               AS debit_int,
    @cast_to_boolean(is_discretionary)                                                                AS is_discretionary,





    @cast_to_integer(client_code)                                                                 AS client_code,
    residence_code::TEXT                                                                          AS residence_code,
    

    @cast_to_boolean(trades_okay)                                                                 AS trades_okay,
    @cast_to_date(open_date, format := '%m/%d/%y', prefer_past := True)                           AS open_date,
    closed_reason::TEXT                                                                           AS closed_reason,

    @cast_to_date(last_trade_date, '%m/%d/%y') AS last_trade_date,
    @cast_to_date(crm2_inception_date, '%m/%d/%y') AS crm2_inception_date,


    @cast_to_numeric(market_value, 'negative_suffix' := 'CR') AS market_value,
    @cast_to_numeric(equity, 'negative_prefix' := 'CR') AS equity,
    @cast_to_numeric(loan_value, 'negative_suffix' := 'CR') AS loan_value,

    @cast_to_numeric(settle_date_balance, 'positive_suffix' := 'CR') AS settle_date_balance,
    @cast_to_numeric(trade_date_balance, 'positive_suffix' := 'CR') AS trade_date_balance,


    __ingested_at,
    __data_snapshot_date
  FROM bronze.account
),

deduplicated AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY (account_number, __data_snapshot_date) 
      ORDER BY __ingested_at DESC
    ) AS __rn
  FROM transformed
)
SELECT
  *
  EXCEPT (__rn)

FROM deduplicated
WHERE __rn = 1

