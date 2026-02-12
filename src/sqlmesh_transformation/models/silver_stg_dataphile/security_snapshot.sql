MODEL (
  name silver_dataphile.securities_snapshot,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column @{sys_col_data_snapshot_date},
    batch_size 1,
  ),
);

WITH transformed AS (
  SELECT
    @clean_cusip(cusip)                                                                           AS cusip,
    comment::TEXT                                                                                 AS comment,


    @{sys_col_ingested_at},
    @{sys_col_data_snapshot_date}
  FROM bronze.securities
),

deduplicated AS (
  @deduplicate(
    transformed,
    partition_by := (cusip, @{sys_col_data_snapshot_date}),
    order_by := ["@{sys_col_ingested_at} DESC"]
  )
)
SELECT * FROM deduplicated;