MODEL (
  name silver_dataphile.transaction_cancellation_pairs,
  kind FULL,
  description 'Row-level mapping of rows selected into cancellation pairs.',
  audits (pairs_are_one_to_one, pairs_net_to_zero)
);

WITH tx AS (
  SELECT
    cancel_match_key,
    row_id,
    is_cancelled,
    description,
    time_sequencer,
    quantity,
    cash_amount,
    cost
  FROM silver_dataphile.transactions_enriched
),

keys AS (
  SELECT DISTINCT cancel_match_key
  FROM tx
  WHERE is_cancelled = TRUE
),

scoped AS (
  SELECT t.*
  FROM tx t
  JOIN keys k ON k.cancel_match_key = t.cancel_match_key
),

anchors AS (
  SELECT *
  FROM scoped
  WHERE is_cancelled = TRUE
),

candidates AS (
  SELECT
    a.cancel_match_key,
    a.row_id AS anchor_row_id,
    b.row_id AS cand_row_id,

    /* Priority 1: cancelled ↔ cancelled */
    CASE WHEN b.is_cancelled THEN 0 ELSE 1 END AS s_cancelled,

    /* Priority 2: same description */
    CASE WHEN coalesce(a.description, '') = coalesce(b.description, '') THEN 0 ELSE 1 END AS s_desc,

    /* Deterministic tie-breakers */
    abs(a.time_sequencer - b.time_sequencer) AS s_time,
    b.time_sequencer AS s_cand_time,
    b.row_id AS s_cand_row_id

  FROM anchors a
  JOIN scoped b
    ON b.cancel_match_key = a.cancel_match_key
   AND b.row_id <> a.row_id
   AND b.quantity    = -a.quantity
   AND b.cash_amount = -a.cash_amount
   AND b.cost        = -a.cost
),

ranked AS (
  SELECT
    *,
    row_number() OVER (
      PARTITION BY cancel_match_key, anchor_row_id
      ORDER BY s_cancelled, s_desc, s_time, s_cand_time, s_cand_row_id
    ) AS r_anchor,
    row_number() OVER (
      PARTITION BY cancel_match_key, cand_row_id
      ORDER BY s_cancelled, s_desc, s_time, anchor_row_id
    ) AS r_cand
  FROM candidates
),

pairs AS (
  SELECT
    cancel_match_key,
    least(anchor_row_id, cand_row_id)    AS row_id_1,
    greatest(anchor_row_id, cand_row_id) AS row_id_2
  FROM ranked
  WHERE r_anchor = 1 AND r_cand = 1
  GROUP BY 1, 2, 3
)

SELECT
  cancel_match_key,
  row_id_1 AS row_id,
  md5(concat_ws('|', cancel_match_key, row_id_1, row_id_2)) AS pair_id
FROM pairs

UNION ALL

SELECT
  cancel_match_key,
  row_id_2 AS row_id,
  md5(concat_ws('|', cancel_match_key, row_id_1, row_id_2)) AS pair_id
FROM pairs
;

AUDIT (name pairs_are_one_to_one);
SELECT cancel_match_key, row_id, COUNT(*) AS cnt
FROM @this_model
GROUP BY 1, 2
HAVING COUNT(*) > 1
;

AUDIT (name pairs_net_to_zero);
WITH j AS (
  SELECT
    p.pair_id,
    t.quantity,
    t.cash_amount,
    t.cost
  FROM @this_model p
  JOIN silver_dataphile.transactions_enriched t
    ON t.cancel_match_key = p.cancel_match_key
   AND t.row_id = p.row_id
)
SELECT
  pair_id
FROM j
GROUP BY 1
HAVING NOT (
  COUNT(*) = 2
  AND SUM(quantity) = 0
  AND SUM(cash_amount) = 0
  AND SUM(cost) = 0
)
;
