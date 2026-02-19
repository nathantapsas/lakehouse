MODEL (
  name silver_dataphile.transaction_cancellation_keys,
  kind FULL,
  description 'Per cancel_match_key cancellation summary (pairing-based) with a human-readable explanation.',
  audits (cancel_keys_are_distinct, in_coverage_cancelled_rows_must_be_pairable)
);

WITH tx AS (
  SELECT
    cancel_match_key,
    row_id,
    trade_date,
    is_cancelled,
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

bucket AS (
  SELECT
    t.cancel_match_key,
    MIN(t.trade_date)                                                                   AS min_trade_date,
    COUNT(*)                                                                            AS total_rows,
    SUM(CASE WHEN t.is_cancelled THEN 1 ELSE 0 END)                                     AS cancelled_rows,

    /* used only for the "everything cancelled" bypass */
    SUM(t.quantity)                                                                     AS bucket_net_qty,
    SUM(t.cash_amount)                                                                  AS bucket_net_cash,
    SUM(t.cost)                                                                         AS bucket_net_cost
  FROM tx t
  JOIN keys k
    ON k.cancel_match_key = t.cancel_match_key
  GROUP BY 1
),

paired AS (
  SELECT
    t.cancel_match_key,

    /* Count ONLY cancelled rows that are in a selected pair */
    SUM(CASE WHEN t.is_cancelled THEN 1 ELSE 0 END)                                     AS paired_cancelled_rows
  FROM silver_dataphile.transactions_enriched t
  JOIN silver_dataphile.transaction_cancellation_pairs p
    ON p.cancel_match_key = t.cancel_match_key
   AND p.row_id = t.row_id
  GROUP BY 1
),

final AS (
  SELECT
    b.cancel_match_key,
    (b.min_trade_date >= @system_start_date)                                            AS is_in_coverage,

    CASE
      WHEN b.min_trade_date < @system_start_date
           AND coalesce(p.paired_cancelled_rows, 0) = b.cancelled_rows
        THEN 'PAIRED_OK_PRE_CUTOVER'

      WHEN b.min_trade_date < @system_start_date
        THEN 'ORPHAN_PRE_CUTOVER'

      WHEN b.cancelled_rows = b.total_rows
           AND b.bucket_net_qty = 0
           AND b.bucket_net_cash = 0
           AND b.bucket_net_cost = 0
        THEN 'RECONCILED_ALL_CANCELLED'

      WHEN coalesce(p.paired_cancelled_rows, 0) = b.cancelled_rows
        THEN 'PAIRED_OK'

      ELSE 'PAIRING_INCOMPLETE'
    END AS cancellation_recon_status,

    CASE
      WHEN b.min_trade_date < @system_start_date
           AND coalesce(p.paired_cancelled_rows, 0) = b.cancelled_rows
        THEN 'Pre-cutover trade date, but a matching reversal was found in the available warehouse data.'

      WHEN b.min_trade_date < @system_start_date
        THEN 'Pre-cutover trade date; a matching reversal was not found in warehouse history.'

      WHEN b.cancelled_rows = b.total_rows
           AND b.bucket_net_qty = 0
           AND b.bucket_net_cash = 0
           AND b.bucket_net_cost = 0
        THEN 'All rows in this group are cancelled and the group nets to zero; treat all as cancelled.'

      WHEN coalesce(p.paired_cancelled_rows, 0) = b.cancelled_rows
        THEN 'Cancelled rows were successfully paired with exact reversals.'

      ELSE 'Some cancelled rows could not be deterministically paired to exact reversals; investigate missing/duplicate legs or key collisions.'
    END                                                                                 AS cancellation_note

  FROM bucket b
  LEFT JOIN paired p
    ON p.cancel_match_key = b.cancel_match_key
)

SELECT
  cancel_match_key,
  is_in_coverage,
  cancellation_recon_status,
  cancellation_note
FROM final
;

AUDIT (name cancel_keys_are_distinct);
SELECT cancel_match_key, COUNT(*)                                                       AS cnt
FROM @this_model
GROUP BY 1
HAVING COUNT(*) > 1
;

AUDIT (name in_coverage_cancelled_rows_must_be_pairable, blocking false);
WITH b AS (
  SELECT
    cancel_match_key,
    MIN(trade_date) AS min_trade_date,
    SUM(CASE WHEN is_cancelled THEN 1 ELSE 0 END)                                       AS cancelled_rows,
    COUNT(*) AS total_rows,
    SUM(quantity)                                                                       AS bucket_net_qty,
    SUM(cash_amount)                                                                    AS bucket_net_cash,
    SUM(cost)                                                                           AS bucket_net_cost
  FROM silver_dataphile.transactions_enriched
  GROUP BY 1
),
p AS (
  SELECT
    x.cancel_match_key,
    SUM(CASE WHEN t.is_cancelled THEN 1 ELSE 0 END)                                     AS paired_cancelled_rows
  FROM silver_dataphile.transaction_cancellation_pairs x
  JOIN silver_dataphile.transactions_enriched t
    ON t.cancel_match_key = x.cancel_match_key
   AND t.row_id = x.row_id
  GROUP BY 1
)
SELECT
  m.cancel_match_key
FROM @this_model m
JOIN b
  ON b.cancel_match_key = m.cancel_match_key
LEFT JOIN p
  ON p.cancel_match_key = m.cancel_match_key
WHERE
  b.min_trade_date >= @system_start_date
  AND NOT (
    /* bypass: all cancelled + bucket net zero */
    b.cancelled_rows = b.total_rows
    AND b.bucket_net_qty = 0
    AND b.bucket_net_cash = 0
    AND b.bucket_net_cost = 0
  )
  AND coalesce(p.paired_cancelled_rows, 0) < b.cancelled_rows;
