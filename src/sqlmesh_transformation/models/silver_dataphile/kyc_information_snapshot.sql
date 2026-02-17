MODEL (
  name silver_dataphile.kyc_information_snapshot,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column as_of_date,
    batch_size 1,
  ),
  depends_on (silver_dataphile.clients_snapshot),  -- Required for the foreign key audit
  audits (
    assert_foreign_key_same_day (
      parent_model := silver_dataphile.clients_snapshot,
      mappings := [(client_code, client_code)],
      child_time_column := as_of_date,
      parent_time_column := as_of_date,
      blocking := false
    )
  ),
);

WITH src AS (
  SELECT
    k.*,
    m.as_of_date
  FROM bronze.kyc_information k
  JOIN bronze_meta.dataphile_asof_map m
    ON k.@{sys_col_data_snapshot_date} = m.@{sys_col_data_snapshot_date}
),

transformed AS (
  SELECT
    @cast_to_integer(client_code) AS client_code,

    -- @cast_to_date(last_trade_date, '%m/%d/%y') AS last_trade_date,
    @cast_to_date(date_met, format := '%m/%d/%y', prefer_past := True) AS date_met,

    -- @cast_to_numeric(income) AS income,
    -- @cast_to_numeric(net_worth) AS net_worth,
    @cast_to_numeric(fixed_assets) AS fixed_assets,
    @{sys_col_ingested_at},
    @{sys_col_data_snapshot_date},
    as_of_date

  FROM src
  WHERE as_of_date BETWEEN @start_ds AND @end_ds
),

deduplicated AS (
  @deduplicate(
    transformed,
    partition_by := (client_code, as_of_date),
    order_by := ["@{sys_col_ingested_at} DESC"]
  )
)
SELECT * FROM deduplicated;