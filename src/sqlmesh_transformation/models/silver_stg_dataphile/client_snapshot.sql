MODEL (
  name silver_stg_dataphile.client_snapshot,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column __data_snapshot_date,
    batch_size 1,
  ),
  grain (client_code, __data_snapshot_date),
);

WITH transformed AS (
  SELECT
    @cast_to_integer(client_code) AS client_code,
    ia_code,
    first_name,
    last_name,

    @cast_to_date(open_date, format := '%m/%d/%y', prefer_past := True) AS open_date,
    @cast_to_date(close_date, format := '%m/%d/%y', prefer_past := True) AS close_date,
    @cast_to_date(birth_date, format := '%m/%d/%Y', prefer_past := True) AS birth_date,

    @cast_to_boolean(consolidate) AS consolidate,

    __ingested_at,
    __data_snapshot_date
  FROM bronze.client
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
