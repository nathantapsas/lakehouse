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
  SELECT 
    t.*,
    /* 
       Determine "Side" for pairing (1 for Positive, -1 for Negative).
       Priority: Quantity -> Cash -> Cost.
       The cancel_match_key ensures magnitudes are identical, so we just need to separate opposites.
    */
    CASE
      WHEN quantity > 0 THEN 1
      WHEN quantity < 0 THEN -1
      WHEN cash_amount > 0 THEN 1
      WHEN cash_amount < 0 THEN -1
      WHEN cost > 0 THEN 1
      WHEN cost < 0 THEN -1
      ELSE 0
    END AS side
  FROM tx t
  JOIN keys k ON k.cancel_match_key = t.cancel_match_key
),

ranked AS (
  SELECT
    *,
    /* 
       Rank rows within each side to facilitate deterministic 1-to-1 matching.
       Ordering priorities:
       1. Cancelled rows (Consume these first).
       2. Description (Heuristic: identical descriptions are better matches).
       3. Time Sequencer (Heuristic: maintain chronological alignment).
       4. Row ID (Deterministic tie-breaker).
    */
    row_number() OVER (
      PARTITION BY cancel_match_key, side
      ORDER BY 
        CASE WHEN is_cancelled THEN 0 ELSE 1 END,
        coalesce(description, ''),
        time_sequencer,
        row_id
    ) AS match_rank
  FROM scoped
  WHERE side <> 0
),

pairs AS (
  SELECT
    a.cancel_match_key,
    a.row_id AS row_id_1,
    b.row_id AS row_id_2
  FROM ranked a
  JOIN ranked b
    ON a.cancel_match_key = b.cancel_match_key
   AND a.side = 1          -- Positive Side
   AND b.side = -1         -- Negative Side
   AND a.match_rank = b.match_rank
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
