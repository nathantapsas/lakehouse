MODEL (
  name silver_dataphile.kyc_information_snapshot,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column @{sys_col_data_snapshot_date},
    batch_size 1,
  ),
  depends_on (silver_dataphile.clients_snapshot),  -- Required for the foreign key audit
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
    @cast_to_integer(client_code) AS client_code,

    -- @cast_to_date(last_trade_date, '%m/%d/%y') AS last_trade_date,
    @cast_to_date(date_met, format := '%m/%d/%y', prefer_past := True) AS date_met,

    -- @cast_to_numeric(income) AS income,
    -- @cast_to_numeric(net_worth) AS net_worth,
    @cast_to_numeric(fixed_assets) AS fixed_assets,
    @{sys_col_ingested_at},
    @{sys_col_data_snapshot_date}

  FROM bronze.kyc_information
),

deduplicated AS (
  @deduplicate(
    transformed,
    partition_by := (client_code, @{sys_col_data_snapshot_date}),
    order_by := ["@{sys_col_ingested_at} DESC"]
  )
)
SELECT * FROM deduplicated;