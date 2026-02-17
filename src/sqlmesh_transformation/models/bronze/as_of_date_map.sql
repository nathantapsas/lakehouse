MODEL (
  name bronze_meta.dataphile_asof_map,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column file_date,
    batch_size 1
  ),
);

WITH tx AS (
  SELECT
    @cast_to_date(process_date, format := '%m/%d/%y', prefer_past := True)  AS as_of_date,
    @{sys_col_data_snapshot_date}                                           AS file_date
  FROM bronze.transactions
  WHERE as_of_date BETWEEN @start_ds AND @end_ds
),

as_of_to_first_file AS (
  SELECT
    as_of_date,
    MIN(file_date) AS first_file_date
  FROM tx
  WHERE as_of_date IS NOT NULL AND file_date IS NOT NULL
  GROUP BY 1
)


SELECT
  first_file_date AS file_date,
  as_of_date,
  CURRENT_TIMESTAMP AS computed_at
FROM as_of_to_first_file;
