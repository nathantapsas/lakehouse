MODEL (
  name silver_dataphile.associated_parties_snapshot,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column __data_as_of_date,
  ),
  depends_on (silver_dataphile.clients_snapshot),  -- Required for the foreign key audit
  audits (
    assert_foreign_key_same_day (
      parent_model := silver_dataphile.clients_snapshot,
      mappings := [(client_code, client_code)],
      child_time_column := __data_as_of_date,
      parent_time_column := __data_as_of_date,
      blocking := false
    )
  ),
);

WITH date_map AS (
  SELECT
    @{sys_col_data_snapshot_date},
    data_as_of_date                                                         AS __data_as_of_date
  FROM bronze_meta.dataphile_asof_map
    WHERE data_as_of_date BETWEEN @start_ds AND @end_ds
),

transformed AS (
  SELECT
    @cast_to_integer(client_code)                                           AS client_code,
    @cast_to_integer(sequence_number)                                       AS sequence_number,
    title::TEXT                                                             AS title,

    @cast_to_integer(address_number) AS address_number,
    first_name::TEXT                                                        AS first_name,
    last_name::TEXT                                                         AS last_name,

    citizenship::TEXT                                                       AS citizenship,
    residence_code::TEXT                                                    AS residence_code,
    -- TODO: Rename this
    recipient_code::TEXT AS recipient_code,

    uci::TEXT                                                               AS uci,
    @cast_to_numeric(uci_percentage)                                        AS uci_percentage,
    @cast_to_date(birth_date, format := '%m/%d/%Y')                         AS birth_date,
    sin::TEXT                                                               AS sin,
    ssn::TEXT                                                               AS ssn,
    foreign_taxpayer_id::TEXT                                               AS foreign_taxpayer_id,
    us_taxpayer_id_type::TEXT                                               AS us_taxpayer_id_type,
    -- TODO: Rename this in the source
    @cast_to_boolean(w9_on_file)                                            AS has_w9_on_file,
    -- TODO: Rename this in the source
    @cast_to_date(w9_date_received, format := '%m/%d/%y', prefer_past := true) AS w9_received_date,
    -- TODO: Rename this
    UPPER(TRIM(limitation_of_benefits::TEXT))                               AS limitation_of_benefits,
    -- TODO: Rename this in the source
    @clean_status(record_status)                                            AS status,



    @{sys_col_ingested_at},
    __data_as_of_date

  FROM bronze.associated_parties a
  JOIN date_map
    ON a.@{sys_col_data_snapshot_date} = date_map.@{sys_col_data_snapshot_date}
),

deduplicated AS (
  @deduplicate(
    transformed,
    partition_by := (client_code, sequence_number, __data_as_of_date),
    order_by := ["@{sys_col_ingested_at} DESC"]
  )
)
SELECT * FROM deduplicated;
