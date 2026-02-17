MODEL (
  name silver_dataphile.trades_snapshot,
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
      parent_model := silver_dataphile.accounts_snapshot,
      mappings := [(other_side_account_number, account_number)],
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
  ),
  allow_partials: true
);

WITH transformed AS (
  SELECT
    @cast_to_integer(trade_number)                                                                AS trade_number,
    @clean_account_number(account_number)                                                         AS account_number,
    @clean_account_number(other_side_account_number)                                              AS other_side_account_number,
    @clean_cusip(cusip)                                                                           AS cusip,
    @cast_to_date(process_date, format := '%m/%d/%y', prefer_past := True)                        AS process_date,

    @cast_to_numeric(quantity) AS quantity,



    @{sys_col_ingested_at},
    @{sys_col_data_snapshot_date}
  FROM bronze.trades
  WHERE @{sys_col_data_snapshot_date} BETWEEN @start_ds AND @end_ds
),

deduplicated AS (
  @deduplicate(
    transformed,
    partition_by := (trade_number, process_date, @{sys_col_data_snapshot_date}),
    order_by := ["@{sys_col_ingested_at} DESC"]
  )
)
SELECT * FROM deduplicated;