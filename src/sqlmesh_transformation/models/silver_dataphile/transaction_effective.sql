MODEL (
  name gold.transactions_effective,
  kind VIEW,
  description 'Transactions with is_cancelled_effective derived from row-level pairing, plus a human-readable cancellation note.'
);

SELECT
  t.*,
  k.cancellation_recon_status,
  k.cancellation_note,

  CASE
    WHEN t.is_cancelled THEN TRUE
    WHEN p.row_id IS NOT NULL THEN TRUE
    /* bypass: everything in the group is cancelled + nets to zero */
    WHEN k.cancellation_recon_status = 'RECONCILED_ALL_CANCELLED' THEN TRUE
    ELSE FALSE
  END AS is_cancelled_effective

FROM silver_dataphile.transactions_enriched t
LEFT JOIN silver_dataphile.transaction_cancellation_pairs p
  ON p.cancel_match_key = t.cancel_match_key
 AND p.row_id = t.row_id
LEFT JOIN silver_dataphile.transaction_cancellation_keys k
  ON k.cancel_match_key = t.cancel_match_key
;
