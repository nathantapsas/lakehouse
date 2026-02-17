MODEL (
  name bronze_meta.dataphile_asof_map,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column @{sys_col_data_snapshot_date},
    batch_size 1
  ),
  allow_partials true,
  audits (
    -- basic integrity
    not_null(columns := (as_of_date, @{sys_col_data_snapshot_date})),
    unique_values(columns := (@{sys_col_data_snapshot_date})),

    -- strict mapping expectations
    asof_not_after_snapshot,
    asof_lag_within_limit(max_lag_days := 7),
    asof_has_strong_majority(min_share := 0.80),
    asof_has_no_mode_ties,
    mapping_is_monotonic
  )
);

WITH cleaned AS (
  SELECT
    CAST(@{sys_col_data_snapshot_date} AS DATE) AS @{sys_col_data_snapshot_date},
    @cast_to_date(process_date, format := '%m/%d/%y', prefer_past := true) AS process_date
  FROM bronze.transactions
  WHERE CAST(@{sys_col_data_snapshot_date} AS DATE) BETWEEN @start_ds AND @end_ds
),

counts AS (
  -- count transactions by (file_date, process_date)
  SELECT
    @{sys_col_data_snapshot_date},
    process_date,
    COUNT(*) AS n
  FROM cleaned
  WHERE process_date IS NOT NULL
  GROUP BY 1, 2
),

ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY @{sys_col_data_snapshot_date}
      ORDER BY n DESC, process_date ASC
    ) AS rn,
    -- detect ties: if the top 2 rows have the same n, that's ambiguous
    LEAD(n) OVER (
      PARTITION BY @{sys_col_data_snapshot_date}
      ORDER BY n DESC, process_date ASC
    ) AS next_n,
    SUM(n) OVER (PARTITION BY @{sys_col_data_snapshot_date}) AS total_n
  FROM counts
),

chosen AS (
  SELECT
    @{sys_col_data_snapshot_date},
    process_date AS as_of_date,
    n AS as_of_n,
    total_n,
    CAST(n AS DOUBLE) / NULLIF(total_n, 0) AS as_of_share,
    next_n
  FROM ranked
  WHERE rn = 1
)

SELECT
  @{sys_col_data_snapshot_date},
  as_of_date,
  as_of_n,
  total_n,
  as_of_share
FROM chosen
;

--------------------------------------------------------------------------------
-- INLINE AUDITS (defined in the same file)
-- SQLMesh allows this pattern directly in the SQL model file. :contentReference[oaicite:2]{index=2}
--------------------------------------------------------------------------------

AUDIT (name asof_not_after_snapshot);
SELECT *
FROM @this_model
WHERE as_of_date > @{sys_col_data_snapshot_date};

AUDIT (name asof_lag_within_limit);
-- default max_lag_days if not supplied
SELECT *
FROM @this_model
WHERE DATE_DIFF('day', as_of_date, @{sys_col_data_snapshot_date}) > @max_lag_days
   OR DATE_DIFF('day', as_of_date, @{sys_col_data_snapshot_date}) < 0;

AUDIT (name asof_has_strong_majority);
-- If process_date sometimes drifts +/- 1 day, this catches “mixed” files.
-- Tune min_share as needed (e.g., 0.90 once stable).
SELECT *
FROM @this_model
WHERE as_of_share < @min_share;

AUDIT (name asof_has_no_mode_ties);
-- Recompute tie condition from source counts for the interval being audited.
WITH src AS (
  SELECT
    CAST(@{sys_col_data_snapshot_date} AS DATE) AS @{sys_col_data_snapshot_date},
    @cast_to_date(process_date, format := '%m/%d/%y', prefer_past := true) AS process_date
  FROM bronze.transactions
),
counts AS (
  SELECT
    @{sys_col_data_snapshot_date},
    process_date,
    COUNT(*) AS n
  FROM src
  WHERE @{sys_col_data_snapshot_date} BETWEEN @start_ds AND @end_ds
    AND process_date IS NOT NULL
  GROUP BY 1, 2
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY @{sys_col_data_snapshot_date}
      ORDER BY n DESC, process_date ASC
    ) AS rn,
    LEAD(n) OVER (
      PARTITION BY @{sys_col_data_snapshot_date}
      ORDER BY n DESC, process_date ASC
    ) AS next_n
  FROM counts
)
SELECT *
FROM ranked
WHERE rn = 1 AND next_n = n;

AUDIT (name mapping_is_monotonic);
-- Ensures mapping doesn’t “go backwards” as file dates increase.
WITH ordered AS (
  SELECT
    @{sys_col_data_snapshot_date},
    as_of_date,
    LAG(as_of_date) OVER (ORDER BY @{sys_col_data_snapshot_date}) AS prev_as_of_date
  FROM @this_model
  WHERE @{sys_col_data_snapshot_date} BETWEEN @start_ds AND @end_ds
)
SELECT *
FROM ordered
WHERE prev_as_of_date IS NOT NULL
  AND as_of_date < prev_as_of_date;
