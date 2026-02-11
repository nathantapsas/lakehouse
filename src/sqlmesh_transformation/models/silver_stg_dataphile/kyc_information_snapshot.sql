MODEL (
  name silver_stg_dataphile.kyc_information_snapshot,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column __data_snapshot_date,
    batch_size 1,
  ),
  grain (client_code, __data_snapshot_date),
  depends_on (silver_stg_dataphile.client_snapshot),  -- Required for the foreign key audit
  audits (
    assert_foreign_key_same_day (
      parent_model := silver_stg_dataphile.client_snapshot,
      mappings := [(client_code, client_code)],
      child_time_column := '__data_snapshot_date',
      parent_time_column := '__data_snapshot_date',
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
    __ingested_at,
    __data_snapshot_date

  FROM bronze.kyc_information
),

deduplicated AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY (client_code, __data_snapshot_date) 
      ORDER BY __ingested_at DESC
    ) AS __rn
  FROM transformed
)

SELECT
  *
  EXCEPT (__rn)

FROM deduplicated
WHERE __rn = 1