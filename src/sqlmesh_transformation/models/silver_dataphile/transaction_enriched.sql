MODEL (
  name silver_dataphile.transactions_enriched,
  kind VIEW,
  description 'Transactions snapshot enriched with cancel_match_key and stable row_id for deterministic cancellation pairing.',
  audits (
    tx_required_fields_present_for_key
  )
);

WITH base AS (
  SELECT
    t.*,

    /* cancel_match_key: stable across original + offset, ignores sign */
    md5(
      concat_ws(
        '|',
        coalesce(t.account_number, '<null>'),
        coalesce(t.cusip, '<null>'),
        coalesce(cast(t.trade_date as varchar), '<null>'),
        coalesce(cast(t.settlement_date as varchar), '<null>'),
        coalesce(t.currency, '<null>'),
        coalesce(cast(abs(t.quantity) as varchar), '<null>'),
        coalesce(cast(abs(t.cash_amount) as varchar), '<null>'),
        coalesce(cast(abs(t.cost) as varchar), '<null>'),
        coalesce(
          CASE
            WHEN t.transaction_code IN ('BUY', 'XBY') THEN 'BUY'
            WHEN t.transaction_code IN ('SEL', 'XSL') THEN 'SEL'
            ELSE t.transaction_code
          END,
          '<null>'
        )
      )
    ) AS cancel_match_key

  FROM silver_dataphile.transactions_snapshot t
)

SELECT
  b.*,

  /* row_id: stable per row, used for deterministic pairing + joins */
  md5(
    concat_ws(
      '|',
      coalesce(cast(b.process_date as varchar), '<null>'),
      coalesce(cast(b.time_sequencer as varchar), '<null>'),
      coalesce(b.account_number, '<null>'),
      coalesce(b.cusip, '<null>'),
      coalesce(cast(b.trade_date as varchar), '<null>'),
      coalesce(cast(b.settlement_date as varchar), '<null>'),
      coalesce(b.currency, '<null>'),
      coalesce(b.transaction_code, '<null>'),
      coalesce(cast(b.quantity as varchar), '<null>'),
      coalesce(cast(b.cash_amount as varchar), '<null>'),
      coalesce(cast(b.cost as varchar), '<null>'),
      coalesce(b.journal_reference_number, '<null>'),
      coalesce(b.description, '<null>')
    )
  ) AS row_id

FROM base b
;

AUDIT (name tx_required_fields_present_for_key);
SELECT *
FROM @this_model
WHERE
  account_number IS NULL
  OR transaction_code IS NULL
  OR trade_date IS NULL
  OR settlement_date IS NULL
  OR currency IS NULL
  OR cancel_match_key IS NULL
  OR row_id IS NULL
;
