MODEL (
  name silver_dataphile.associated_parties_snapshot,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column @{sys_col_data_snapshot_date},
    batch_size 1,
  ),
  depends_on (silver_dataphile.clients_snapshot),  -- Required for the foreign key audit
  audits (
    assert_foreign_key_same_day (
      parent_model := silver_dataphile.clients_snapshot,
      mappings := [(client_code, client_code)],
      child_time_column := @{sys_col_data_snapshot_date},
      parent_time_column := @{sys_col_data_snapshot_date},
      blocking := false
    )
  )
);

WITH transformed AS (
  SELECT
    @cast_to_integer(client_code)                                           AS client_code,
    @cast_to_integer(sequence_number)                                       AS sequence_number,

    first_name::TEXT                                                        AS first_name,
    last_name::TEXT                                                         AS last_name,
    title::TEXT                                                             AS title,

    @cast_to_date(birth_date, format := '%m/%d/%Y')                         AS birth_date,

    @{sys_col_ingested_at},
    @{sys_col_data_snapshot_date}


  FROM bronze.associated_parties
),

deduplicated AS (
  @deduplicate(
    transformed,
    partition_by := (client_code, sequence_number, @{sys_col_data_snapshot_date}),
    order_by := ["@{sys_col_ingested_at} DESC"]
  )
)
SELECT * FROM deduplicated;
