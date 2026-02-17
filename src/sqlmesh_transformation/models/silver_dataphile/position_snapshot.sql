MODEL (
  name silver_dataphile.positions_snapshot,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column as_of_date,
    batch_size 1,
  ),
  depends_on (
    silver_dataphile.accounts_snapshot,  -- Required for the foreign key audit
    silver_dataphile.securities_snapshot  -- Required for the foreign key audit
    ), 
  audits (
    assert_foreign_key_same_day (
      parent_model := silver_dataphile.accounts_snapshot,
      mappings := [(account_number, account_number)],
      child_time_column := as_of_date,
      parent_time_column := as_of_date,
      blocking := false
    ),
    assert_foreign_key_same_day (
      parent_model := silver_dataphile.securities_snapshot,
      mappings := [(cusip, cusip)],
      child_time_column := as_of_date,
      parent_time_column := as_of_date,
      blocking := false
    )
  ),
);

WITH src AS (
  SELECT
    p.*,
    m.as_of_date
  FROM bronze.positions p
  JOIN bronze_meta.dataphile_asof_map m
    ON p.@{sys_col_data_snapshot_date} = m.@{sys_col_data_snapshot_date}
),

transformed AS (
  SELECT
    @clean_account_number(account_number)                                                         AS account_number,
    @clean_cusip(cusip)                                                                           AS cusip,

    @cast_to_numeric(current_quantity, 'negative_suffix' := '-')                                  AS current_quantity,
    @cast_to_numeric(safekeeping_quantity, 'negative_suffix' := '-')                              AS safekeeping_quantity,
    @cast_to_numeric(segregated_quantity, 'negative_suffix' := '-')                               AS segregated_quantity,
    @cast_to_numeric(pending_quantity, 'negative_suffix' := '-')                                  AS pending_quantity,
    @cast_to_numeric(client_name_quantity, 'negative_suffix' := '-')                              AS client_name_quantity,
    @cast_to_numeric(memo_quantity, 'negative_suffix' := '-')                                     AS memo_quantity,

    @cast_to_numeric(cost, 'positive_suffix' := 'CR')                                             AS cost,


    @{sys_col_ingested_at},
    @{sys_col_data_snapshot_date},
    as_of_date
  FROM src
  WHERE as_of_date BETWEEN @start_ds AND @end_ds
),

deduplicated AS (
  @deduplicate(
    transformed,
    partition_by := (account_number, cusip, as_of_date),
    order_by := ["@{sys_col_ingested_at} DESC"]
  )
)
SELECT * FROM deduplicated;
