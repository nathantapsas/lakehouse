MODEL (
  name silver_dataphile.restrictions_snapshot,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column as_of_date,
    batch_size 1,
  ),
  depends_on (
    silver_dataphile.clients_snapshot,  -- Required for the foreign key audit
    silver_dataphile.accounts_snapshot  -- Required for the foreign key audit
  ),
  audits (
    assert_foreign_key_same_day (
      parent_model := silver_dataphile.clients_snapshot,
      mappings := [(client_code, client_code)],
      child_time_column := as_of_date,
      parent_time_column := as_of_date,
      blocking := false
    ),
    assert_foreign_key_same_day (
      parent_model := silver_dataphile.accounts_snapshot,
      mappings := [(account_number, account_number)],
      child_time_column := as_of_date,
      parent_time_column := as_of_date,
      blocking := false
    )
  ),
  allow_partials: true
);

@DEF(
  'clean_effect_code',
  (effect_code) -> (
    CASE
      WHEN effect_code IS NULL THEN NULL
      WHEN effect_code = 'W' THEN 'Warning'
      WHEN effect_code = 'E' THEN 'Error'
      ELSE ERROR(printf('Unexpected effect code: %s', effect_code))
    END
  )
);

WITH src AS (
  SELECT
    r.*,
    m.as_of_date
  FROM bronze.restrictions r
  JOIN bronze_meta.dataphile_asof_map m
    ON r.@{sys_col_data_snapshot_date} = m.@{sys_col_data_snapshot_date}
),

transformed AS (
  SELECT
    @cast_to_integer(client_code)                                                                 AS client_code,
    @clean_account_number(account_number)                                                         AS account_number,

    security_class_code::TEXT                                                                     AS security_class_code,

    @cast_to_date(last_date, format := '%m/%d/%y', prefer_past := true)                           AS last_date,
    @cast_to_integer(restriction_code)                                                            AS restriction_code,

    @clean_effect_code(buy_effect_code)                                                           AS buy_effect,
    @clean_effect_code(sell_effect_code)                                                          AS sell_effect,
    @clean_effect_code(credit_effect_code)                                                        AS credit_effect,
    @clean_effect_code(debit_effect_code)                                                         AS debit_effect,
    @clean_effect_code(switch_effect_code)                                                        AS switch_effect,
    @clean_effect_code(transfer_in_effect_code)                                                   AS transfer_in_effect,
    @clean_effect_code(transfer_out_effect_code)                                                  AS transfer_out_effect,

    @{sys_col_ingested_at},
    @{sys_col_data_snapshot_date},
    as_of_date
  FROM src
  WHERE as_of_date BETWEEN @start_ds AND @end_ds
),

deduplicated AS (
  @deduplicate(
    transformed,
    partition_by := (client_code, account_number, security_class_code, restriction_code, as_of_date),
    order_by := ["@{sys_col_ingested_at} DESC"]
  )
)
SELECT * FROM deduplicated;