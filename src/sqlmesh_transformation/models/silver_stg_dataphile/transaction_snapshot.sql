MODEL (
  name silver_dataphile.transactions_snapshot,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column @{sys_col_data_snapshot_date},
    batch_size 1,
  ),
  depends_on (
    silver_dataphile.accounts_snapshot,  -- Required for the foreign key audit
    silver_dataphile.securities_snapshot  -- Required for the foreign key audit
  ), 
  audits (
    assert_foreign_key_same_day (
      parent_model := silver_dataphile.accounts_snapshot,
      mappings := [(account_number, account_number)],
      child_time_column := @{sys_col_data_snapshot_date},
      parent_time_column := @{sys_col_data_snapshot_date},
      blocking := false
    ),
    assert_foreign_key_same_day (
      parent_model := silver_dataphile.securities_snapshot,
      mappings := [(cusip, cusip)],
      child_time_column := @{sys_col_data_snapshot_date},
      parent_time_column := @{sys_col_data_snapshot_date},
      blocking := false
    )
  )
);

WITH transformed AS (
  SELECT
    @clean_cusip(cusip)                                                                           AS cusip,
    @clean_account_number(account_number)                                                         AS account_number,
    sequencer::TEXT                                                                               AS sequencer,
    time_sequencer::TEXT                                                                          AS time_sequencer,
    @cast_to_date(process_date, format := '%m/%d/%y', prefer_past := True)                        AS process_date,
    @clean_currency_code(currency)                                                                AS currency,

    reference_number::TEXT                                                                        AS reference_number,
    @cast_to_numeric(cash_amount, positive_suffix := 'CR')                                        AS cash_amount,
    @cast_to_numeric(quantity, positive_suffix := '-')                                            AS quantity,

    @{sys_col_ingested_at},
    @{sys_col_data_snapshot_date}
  FROM bronze.transactions
),

deduplicated AS (
  @deduplicate(
    transformed,
    partition_by := (
      cusip, 
      account_number, 
      sequencer, 
      time_sequencer, 
      process_date, 
      reference_number,
      @{sys_col_data_snapshot_date}
    ),
    order_by := ["@{sys_col_ingested_at} DESC"]
  )
)
SELECT * FROM deduplicated;

