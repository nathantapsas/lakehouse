AUDIT (
  name assert_foreign_key_scd_same_day,
  dialect duckdb
);

-- Required args:
--   parent_model := <relation>                 -- e.g. silver.client_scd
--:contentReference[oaicite:4]{index=4}arent_col), ...] -- e.g. [(client_code, client_code)]
--
-- Optional args (defaults match SQLMesh SCD conventions):
--   child_valid_from_column := valid_from
--   child_valid_to_column   := valid_to
--   parent_valid_from_column := valid_from
--   parent_valid_to_column   := valid_to
--   as_of_timezone := 'UTC'   -- if you store timestamps in a specific tz, adjust; otherwise leave

@DEF(child_valid_from_column, valid_from);
@DEF(child_valid_to_column,   valid_to);
@DEF(parent_valid_from_column, valid_from);
@DEF(parent_valid_to_column,   valid_to);

WITH
-- Enumerate each snapshot day D being processed by SQLMesh.
-- For daily runs, this will typically be a single day.
days AS (
  SELECT
    gs::DATE AS ds,
    CAST(gs AS TIMESTAMP) AS as_of_ts
  FROM generate_series(CAST(@start_ds AS DATE), CAST(@end_ds AS DATE), INTERVAL 1 DAY) AS t(gs)
),

-- Child rows "present in the snapshot for day D" = child SCD versions effective at as_of_ts.
child_asof AS (
  SELECT
    d.ds AS __audit_ds,
    d.as_of_ts AS __audit_as_of_ts,
    c.*
  FROM @this_model AS c
  JOIN days AS d
    ON c.@child_valid_from_column <= d.as_of_ts
   AND (c.@child_valid_to_column  > d.as_of_ts OR c.@child_valid_to_column IS NULL)
),

-- Parent rows effective at day D.
parent_asof AS (
  SELECT
    d.ds AS __audit_ds,
    d.as_of_ts AS __audit_as_of_ts,
    p.*
  FROM @as_table(@parent_model) AS p
  JOIN days AS d
    ON p.@parent_valid_from_column <= d.as_of_ts
   AND (p.@parent_valid_to_column  > d.as_of_ts OR p.@parent_valid_to_column IS NULL)
),

violations AS (
  SELECT
    c.__audit_ds,
    c.__audit_as_of_ts,
    c.*
  FROM child_asof AS c
  LEFT JOIN parent_asof AS p
    ON p.__audit_ds = c.__audit_ds
   AND @fk_join('c', 'p', @mappings)
  WHERE
    -- Enforce only when the child FK is populated (NULL FKs are not failures).
    @fk_child_non_null('c', @mappings)
    -- Missing parent in the snapshot for that same day D:
    AND p.@parent_valid_from_column IS NULL
)

SELECT *
FROM violations;
