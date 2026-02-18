MODEL (
  name silver_dataphile.securities_snapshot,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column @{sys_col_data_as_of_date},
  ),
);


WITH transformed AS (
  SELECT
    @clean_cusip(cusip)                                                                           AS cusip,

    market::TEXT                                                                                  AS market_code,
    status::TEXT                                                                                  AS status_code,
    security_class_code::TEXT                                                                     AS security_class_code,
    security_name::TEXT                                                                           AS security_name,
    isin::TEXT                                                                                    AS isin,
    @clean_currency_code(currency)                                                                AS currency_code,
    @cast_to_boolean(is_able_to_trade)                                                            AS is_able_to_trade,
    call_put_indicator::TEXT                                                                      AS call_put_indicator,
    primary_symbol::TEXT                                                                          AS primary_symbol,
    @clean_cusip(underlying_cusip)                                                                AS underlying_cusip,
    underlying_symbol::TEXT                                                                       AS underlying_symbol,
    @cast_to_numeric(strike_price)                                                                AS strike_price,
    @cast_to_date(expiry_date, format := '%m/%d/%Y')                                              AS expiry_date,
    incorporation_country_code::TEXT                                                              AS incorporation_country_code,
    @cast_to_boolean(is_us_eci)                                                                   AS is_us_eci,
    @cast_to_numeric(price_factor)                                                                AS price_factor,
    @cast_to_numeric(ask_price)                                                                   AS ask_price,
    @cast_to_numeric(bid_price)                                                                   AS bid_price,
    @cast_to_numeric(last_trade_price)                                                            AS last_trade_price,
    @cast_to_boolean(is_cds_eligible)                                                             AS is_cds_eligible,
    @cast_to_boolean(is_convertible)                                                              AS is_convertible,
    price_status_code::TEXT                                                                       AS price_status_code,
    price_source::TEXT                                                                            AS price_source,

    @cast_to_numeric(loan_value_long_1)                                                           AS loan_value_long_1,
    @cast_to_numeric(loan_value_long_2)                                                           AS loan_value_long_2,
    @cast_to_numeric(loan_value_long_3)                                                           AS loan_value_long_3,
    @cast_to_numeric(loan_value_short_1)                                                          AS loan_value_short_1,
    @cast_to_numeric(loan_value_short_2)                                                          AS loan_value_short_2,
    @cast_to_numeric(loan_value_short_3)                                                          AS loan_value_short_3,

    @cast_to_boolean(has_manual_price)                                                            AS has_manual_price,
    @cast_to_boolean(has_reduced_margin)                                                          AS has_reduced_margin,
    @cast_to_boolean(trades_options)                                                              AS trades_options,

    comment::TEXT                                                                                 AS comment,

    @{sys_col_ingested_at},
    @{sys_col_data_as_of_date}

  FROM bronze.securities
  WHERE @{sys_col_data_as_of_date} BETWEEN @start_ds AND @end_ds
),

deduplicated AS (
  @deduplicate(
    transformed,
    partition_by := (cusip, @{sys_col_data_as_of_date}),
    order_by := ["@{sys_col_ingested_at} DESC"]
  )
)
SELECT * FROM deduplicated;