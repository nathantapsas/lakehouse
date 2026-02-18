MODEL (
  name silver_dataphile.transactions_snapshot,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column process_date,
  ),
  depends_on (
    silver_dataphile.accounts_snapshot,   -- Required for the foreign key audit
    silver_dataphile.securities_snapshot  -- Required for the foreign key audit
  ), 
  audits (
    assert_foreign_key_same_day (
      parent_model := silver_dataphile.accounts_snapshot,
      mappings := [(account_number, account_number)],
      child_time_column := process_date,
      parent_time_column := @{sys_col_data_as_of_date},
      blocking := false
    ),
    assert_foreign_key_same_day (
      parent_model := silver_dataphile.securities_snapshot,
      mappings := [(cusip, cusip)],
      child_time_column := process_date,
      parent_time_column := @{sys_col_data_as_of_date},
      blocking := false
    )
  )
);


WITH transformed AS (
  SELECT
    @clean_cusip(cusip)                                                                           AS cusip,
    @clean_account_number(account_number)                                                         AS account_number,

    @cast_to_date(process_date, format := '%m/%d/%y', prefer_past := True)                        AS process_date,
    @cast_to_integer(sequencer)                                                                   AS sequencer,
    @cast_to_integer(time_sequencer)                                                              AS time_sequencer,
    @clean_currency_code(currency)                                                                AS currency,
    @cast_to_numeric(cash_amount, positive_suffix := 'CR')                                        AS cash_amount,
    @cast_to_numeric(exchange_rate)                                                               AS exchange_rate, 

    ia_code::TEXT                                                                                 AS ia_code,

    description::TEXT                                                                             AS description,

    journal_reference_number::TEXT                                                                AS journal_reference_number,
    @cast_to_numeric(quantity, positive_suffix := '-')                                            AS quantity,
    @cast_to_date(settlement_date, format := '%m/%d/%y', prefer_past := False)                    AS settlement_date,
    @cast_to_date(trade_date, format := '%m/%d/%y', prefer_past := False)                         AS trade_date,
    @cast_to_boolean(transaction_cancelled)                                                       AS is_cancelled,
    -- @cast_to_numeric(cost, negative_prefix := '-')                                                AS cost,

    @split_part(transaction_code, '-', 1)                                                         AS transaction_code,
    @split_part(transaction_code, '-', 2)                                                         AS transaction_code_label,

    @{sys_col_ingested_at},
    @{sys_col_data_as_of_date}

    FROM bronze.transactions
    WHERE @{sys_col_data_as_of_date} BETWEEN @start_ds AND @end_ds  

),

deduplicated AS (
  @deduplicate(
    transformed,
    partition_by := (
      process_date, 
      account_number, 
      journal_reference_number,
      cusip, 
      sequencer, 
      time_sequencer, 
    ),
    order_by := ["@{sys_col_ingested_at} DESC"]
  )
)
SELECT * FROM deduplicated;

