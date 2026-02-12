MODEL (
  name silver_dataphile.addresses_snapshot,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column @{sys_col_data_snapshot_date},
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
      child_time_column := @{sys_col_data_snapshot_date},
      parent_time_column := @{sys_col_data_snapshot_date},
      blocking := false
    ),
    assert_foreign_key_same_day (
      parent_model := silver_dataphile.accounts_snapshot,
      mappings := [(account_number, account_number)],
      child_time_column := @{sys_col_data_snapshot_date},
      parent_time_column := @{sys_col_data_snapshot_date},
      blocking := false
    )
  )
);

WITH transformed AS (
  SELECT
    @cast_to_integer(client_code)                                                                 AS client_code,
    @cast_to_integer(sequence_number)                                                             AS sequence_number,
    @clean_account_number(account_number)                                                         AS account_number,

    line_1::TEXT                                                                                  AS line_1,
    line_2::TEXT                                                                                  AS line_2,
    line_3::TEXT                                                                                  AS line_3,
    line_4::TEXT                                                                                  AS line_4,
    line_5::TEXT                                                                                  AS line_5,
    line_6::TEXT                                                                                  AS line_6,
    line_7::TEXT                                                                                  AS line_7,
    line_8::TEXT                                                                                  AS line_8,


    @{sys_col_ingested_at},
    @{sys_col_data_snapshot_date}
  FROM bronze.addresses
),

deduplicated AS (
  @deduplicate(
    transformed,
    partition_by := (client_code, account_number, sequence_number, @{sys_col_data_snapshot_date}),
    order_by := ["@{sys_col_ingested_at} DESC"]
  )
)
SELECT * FROM deduplicated;