MODEL (
  name silver_dataphile.trades_snapshot,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column process_date,
  ),
  depends_on (
    silver_dataphile.accounts_snapshot,  -- Required for the foreign key audit
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
      parent_model := silver_dataphile.accounts_snapshot,
      mappings := [(other_side_account_number, account_number)],
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
  ),
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
    @{sys_col_data_as_of_date}

  FROM bronze.trades
  WHERE process_date BETWEEN @start_ds AND @end_ds
),

deduplicated AS (
  @deduplicate(
    transformed,
    -- TODO: double check the primary key for this model
    partition_by := (trade_number, process_date),
    order_by := ["@{sys_col_ingested_at} DESC"]
  )
)
SELECT * FROM deduplicated;