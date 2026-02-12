MODEL (
  name silver_dataphile.crs_records_snapshot,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column @{sys_col_data_snapshot_date},
    batch_size 1,
  ),
  depends_on (
    silver_dataphile.clients_snapshot,           -- Required for the foreign key audit
    silver_dataphile.associated_parties_snapshot  -- Required for the foreign key audit
  ),
  audits (
    assert_foreign_key_same_day (
      parent_model := silver_dataphile.clients_snapshot,
      -- TODO: Change this to client CODE
      mappings := [(client_code, client_id)],
      child_time_column := @{sys_col_data_snapshot_date},
      parent_time_column := @{sys_col_data_snapshot_date},
      blocking := false
    ),
    -- assert_foreign_key_same_day (
    --   parent_model := silver_dataphile.associated_party_snapshot,
    --   mappings := [(client_code, client_code), (associated_party_sequence_number, sequence_number)],
    --   child_time_column := @{sys_col_data_snapshot_date},
    --   parent_time_column := @{sys_col_data_snapshot_date},
    --   blocking := false
    -- )
  )
);

WITH transformed AS (
  SELECT
    -- TODO: CAST THIS TO INT ONCE IT IS CLIENT CODE NOT ID
    client_code                                                                                   AS client_code,
    @cast_to_integer(associated_party_sequence_number)                                            AS associated_party_sequence_number,
    @cast_to_integer(sequence_number)                                                             AS sequence_number,

    certification_status::TEXT                                                                    AS certification_status,
    citizenship::TEXT                                                                             AS citizenship,
    tax_jurisdiction::TEXT                                                                        AS tax_jurisdiction,
    foreign_taxpayer_id::TEXT                                                                     AS foreign_taxpayer_id,


    @{sys_col_ingested_at},
    @{sys_col_data_snapshot_date}
  FROM bronze.crs_records
),

WITH deduplicated AS (
  @deduplicate(
    transformed,
    partition_by := (client_code, associated_party_sequence_number, sequence_number, @{sys_col_data_snapshot_date}),
    order_by := ["@{sys_col_ingested_at} DESC"]
  )
)
SELECT * FROM deduplicated



