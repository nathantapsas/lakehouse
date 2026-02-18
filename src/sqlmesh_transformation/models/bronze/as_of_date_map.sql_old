MODEL (
  name bronze_meta.dataphile_asof_map,
  kind FULL,
  audits (
    NOT_NULL(columns := (@{sys_col_data_snapshot_date}, reason)),
    UNIQUE_VALUES(columns := (@{sys_col_data_snapshot_date})),

    mapping_asof_null_on_non_extract_days,
    mapping_asof_present_on_extract_days,
    mapping_asof_before_snapshot,
    mapping_lag_within_limit(max_lag_days := 14),
    mapping_monotonic_for_extract_days,
    mapping_covers_all_bronze_snapshot_dates
  )
);

-- -----------------------------------------------------------------------------
-- EXPECTED HOLIDAY TABLE SHAPE
-- bronze.holidays with at least:
--   holiday_date   (text)  -- mm/dd/yy
--   region_code    (text)  -- CAN or USA
--   type_code      (text)  -- B, T, N
--
-- RULE:
--   no extracts ⇔ weekend OR (CAN is B AND USA is B)
-- -----------------------------------------------------------------------------

WITH holiday_src AS (
  SELECT
    @cast_to_date(holiday_date, format := '%m/%d/%y', prefer_past := false) AS holiday_date,
    region_code                                                             AS region_code,
    type_code                                                               AS type_code
  FROM bronze.holidays
  WHERE region_code IN ('CAN', 'USA')
),

bounds AS (
  SELECT
    CAST(MIN(@{sys_col_data_snapshot_date}) AS DATE) AS min_snapshot_date,
    CAST(MAX(@{sys_col_data_snapshot_date}) AS DATE) AS max_snapshot_date
  FROM bronze.transactions
),

date_spine AS (
  SELECT d::DATE AS d
  FROM generate_series(
    (SELECT min_snapshot_date - INTERVAL 60 DAY FROM bounds),
    (SELECT max_snapshot_date + INTERVAL 60 DAY FROM bounds),
    INTERVAL 1 DAY
  ) AS t(d)
),

calendar AS (
  SELECT
    s.d AS @{sys_col_data_snapshot_date},

    (EXTRACT('dow' FROM s.d) IN (0, 6)) AS is_weekend,

    EXISTS (
      SELECT 1 FROM holiday_src h
      WHERE h.holiday_date = s.d AND h.region_code = 'CAN' AND h.type_code = 'B'
    ) AS can_closed_b,

    EXISTS (
      SELECT 1 FROM holiday_src h
      WHERE h.holiday_date = s.d AND h.region_code = 'USA' AND h.type_code = 'B'
    ) AS usa_closed_b
  FROM date_spine s
),

calendar2 AS (
  SELECT
    @{sys_col_data_snapshot_date},
    is_weekend,
    (can_closed_b AND usa_closed_b) AS both_markets_closed_b,
    NOT is_weekend AND NOT (can_closed_b AND usa_closed_b) AS is_extract_day
  FROM calendar
),

calendar_with_prev_extract AS (
  SELECT
    c.*,
    MAX(CASE WHEN c.is_extract_day THEN c.@{sys_col_data_snapshot_date} END)
      OVER (
        ORDER BY c.@{sys_col_data_snapshot_date}
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
      ) AS prev_extract_day
  FROM calendar2 c
),

mapped AS (
  SELECT
    @{sys_col_data_snapshot_date},
    is_extract_day,
    is_weekend,
    both_markets_closed_b,
    prev_extract_day,
    CASE WHEN is_extract_day THEN prev_extract_day ELSE NULL END AS data_as_of_date
  FROM calendar_with_prev_extract
),

mapped_with_lag AS (
  SELECT
    *,
    CASE
      WHEN data_as_of_date IS NULL THEN NULL
      ELSE DATE_DIFF('day', data_as_of_date, @{sys_col_data_snapshot_date})
    END AS lag_days
  FROM mapped
),

skipped_days AS (
  SELECT
    m.@{sys_col_data_snapshot_date},
    m.data_as_of_date,
    c.@{sys_col_data_snapshot_date} AS skipped_date,
    c.is_weekend,
    c.both_markets_closed_b
  FROM mapped_with_lag m
  JOIN calendar2 c
    ON c.@{sys_col_data_snapshot_date} > m.data_as_of_date
   AND c.@{sys_col_data_snapshot_date} < m.@{sys_col_data_snapshot_date}
  WHERE m.is_extract_day = TRUE
    AND m.data_as_of_date IS NOT NULL
    AND m.lag_days > 1
    AND (c.is_weekend = TRUE OR c.both_markets_closed_b = TRUE)
),

skipped_summary AS (
  SELECT
    @{sys_col_data_snapshot_date},
    data_as_of_date,
    STRING_AGG(
      CAST(skipped_date AS TEXT) ||
      ' (' ||
      TRIM(
        CASE WHEN is_weekend THEN 'weekend' ELSE '' END ||
        CASE WHEN is_weekend AND both_markets_closed_b THEN '; ' ELSE '' END ||
        CASE WHEN both_markets_closed_b THEN 'both CAN & USA markets closed (Type=B)' ELSE '' END
      ) ||
      ')',
      ', ' ORDER BY skipped_date
    ) AS skipped_details
  FROM skipped_days
  GROUP BY 1, 2
),

final AS (
  SELECT
    m.@{sys_col_data_snapshot_date},
    m.data_as_of_date,
    CASE
      WHEN m.is_extract_day = FALSE THEN
        'No extract expected on ' || CAST(m.@{sys_col_data_snapshot_date} AS TEXT) ||
        ' because ' ||
        TRIM(
          CASE WHEN m.is_weekend THEN 'it is a weekend day' ELSE '' END ||
          CASE WHEN m.is_weekend AND m.both_markets_closed_b THEN '; ' ELSE '' END ||
          CASE WHEN m.both_markets_closed_b THEN 'both CAN & USA markets are fully closed (Type=B)' ELSE '' END
        ) ||
        '. Therefore data_as_of_date is NULL by design.'

      WHEN m.data_as_of_date IS NULL THEN
        'Extract day ' || CAST(m.@{sys_col_data_snapshot_date} AS TEXT) ||
        ' has no prior extract day available in the generated range, so data_as_of_date is NULL. ' ||
        'Increase the date_spine buffer if this occurs unexpectedly.'

      WHEN m.lag_days = 1 THEN
        'Normal delivery: the extract dated ' || CAST(m.@{sys_col_data_snapshot_date} AS TEXT) ||
        ' contains end-of-day data for the previous extract day (' || CAST(m.data_as_of_date AS TEXT) || ').'

      ELSE
        'Delayed delivery: the extract dated ' || CAST(m.@{sys_col_data_snapshot_date} AS TEXT) ||
        ' maps to data_as_of_date ' || CAST(m.data_as_of_date AS TEXT) ||
        ' because extracts were not delivered on the intervening date(s): ' ||
        COALESCE(s.skipped_details, '(no skipped-day details found)') ||
        '. This produces a lag of ' || CAST(m.lag_days AS TEXT) || ' day(s).'
    END AS reason
  FROM mapped_with_lag m
  LEFT JOIN skipped_summary s
    ON s.@{sys_col_data_snapshot_date} = m.@{sys_col_data_snapshot_date}
   AND s.data_as_of_date = m.data_as_of_date
)

SELECT
  @{sys_col_data_snapshot_date},
  data_as_of_date,
  reason
FROM final
ORDER BY @{sys_col_data_snapshot_date}
;

-- =============================================================================
-- INLINE AUDITS (standalone: DO NOT reference model CTEs like date_spine)
-- =============================================================================

AUDIT (name mapping_asof_null_on_non_extract_days);
WITH holiday_src AS (
  SELECT
    @cast_to_date(holiday_date, format := '%m/%d/%y', prefer_past := false) AS holiday_date,
    region_code                                                             AS region_code,
    type_code                                                               AS type_code
  FROM bronze.holidays
  WHERE region_code IN ('CAN', 'USA')
),
spine AS (
  SELECT d::DATE AS d
  FROM generate_series(
    (SELECT MIN(@{sys_col_data_snapshot_date}) FROM @this_model),
    (SELECT MAX(@{sys_col_data_snapshot_date}) FROM @this_model),
    INTERVAL 1 DAY
  ) AS t(d)
),
cal AS (
  SELECT
    s.d AS snap,
    (EXTRACT('dow' FROM s.d) IN (0, 6)) AS is_weekend,
    EXISTS (SELECT 1 FROM holiday_src h WHERE h.holiday_date=s.d AND h.region_code='CAN' AND h.type_code='B') AS can_closed_b,
    EXISTS (SELECT 1 FROM holiday_src h WHERE h.holiday_date=s.d AND h.region_code='USA' AND h.type_code='B') AS usa_closed_b
  FROM spine s
),
flag AS (
  SELECT
    snap,
    NOT is_weekend AND NOT (can_closed_b AND usa_closed_b) AS is_extract_day
  FROM cal
)
SELECT m.*
FROM @this_model m
JOIN flag f ON f.snap = m.@{sys_col_data_snapshot_date}
WHERE f.is_extract_day = FALSE
  AND m.data_as_of_date IS NOT NULL;

AUDIT (name mapping_asof_present_on_extract_days);
WITH holiday_src AS (
  SELECT
    @cast_to_date(holiday_date, format := '%m/%d/%y', prefer_past := false) AS holiday_date,
    region_code                                                             AS region_code,
    type_code                                                               AS type_code
  FROM bronze.holidays
  WHERE region_code IN ('CAN', 'USA')
),
spine AS (
  SELECT d::DATE AS d
  FROM generate_series(
    (SELECT MIN(@{sys_col_data_snapshot_date}) FROM @this_model),
    (SELECT MAX(@{sys_col_data_snapshot_date}) FROM @this_model),
    INTERVAL 1 DAY
  ) AS t(d)
),
cal AS (
  SELECT
    s.d AS snap,
    (EXTRACT('dow' FROM s.d) IN (0, 6)) AS is_weekend,
    EXISTS (
      SELECT 1 FROM holiday_src h
      WHERE h.holiday_date = s.d AND h.region_code = 'CAN' AND h.type_code = 'B'
    ) AS can_closed_b,
    EXISTS (
      SELECT 1 FROM holiday_src h
      WHERE h.holiday_date = s.d AND h.region_code = 'USA' AND h.type_code = 'B'
    ) AS usa_closed_b
  FROM spine s
),
flag AS (
  SELECT
    snap,
    NOT is_weekend AND NOT (can_closed_b AND usa_closed_b) AS is_extract_day
  FROM cal
),
first_extract AS (
  SELECT MIN(snap) AS first_extract_snap
  FROM flag
  WHERE is_extract_day = TRUE
)
SELECT m.*
FROM @this_model m
JOIN flag f
  ON f.snap = m.@{sys_col_data_snapshot_date}
CROSS JOIN first_extract fe
WHERE f.is_extract_day = TRUE
  AND m.data_as_of_date IS NULL
  AND m.@{sys_col_data_snapshot_date} <> fe.first_extract_snap;


AUDIT (name mapping_asof_before_snapshot);
SELECT *
FROM @this_model
WHERE data_as_of_date IS NOT NULL
  AND data_as_of_date >= @{sys_col_data_snapshot_date};

AUDIT (name mapping_lag_within_limit);
SELECT *
FROM @this_model
WHERE data_as_of_date IS NOT NULL
  AND (
    DATE_DIFF('day', data_as_of_date, @{sys_col_data_snapshot_date}) < 1
    OR DATE_DIFF('day', data_as_of_date, @{sys_col_data_snapshot_date}) > @max_lag_days
  );

AUDIT (name mapping_monotonic_for_extract_days);
WITH ordered AS (
  SELECT
    @{sys_col_data_snapshot_date},
    data_as_of_date,
    LAG(data_as_of_date) OVER (ORDER BY @{sys_col_data_snapshot_date}) AS prev_as_of
  FROM @this_model
  WHERE data_as_of_date IS NOT NULL
)
SELECT *
FROM ordered
WHERE prev_as_of IS NOT NULL
  AND data_as_of_date < prev_as_of;

AUDIT (name mapping_covers_all_bronze_snapshot_dates);
WITH bronze_days AS (
  SELECT DISTINCT CAST(@{sys_col_data_snapshot_date} AS DATE) AS snap
  FROM bronze.transactions
),
missing AS (
  SELECT b.snap
  FROM bronze_days b
  LEFT JOIN @this_model m
    ON b.snap = m.@{sys_col_data_snapshot_date}
  WHERE m.@{sys_col_data_snapshot_date} IS NULL
)
SELECT * FROM missing;
