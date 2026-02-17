MODEL (
  name silver_dataphile.addresses_snapshot,
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
);

with src as (
  select
    a.*,
    m.as_of_date
  from bronze.addresses a
  join bronze_meta.dataphile_asof_map m
    on a.@{sys_col_data_snapshot_date} = m.@{sys_col_data_snapshot_date}
),

transformed AS (
  SELECT
    @cast_to_integer(client_code)                                                                 AS client_code,
    @cast_to_integer(sequence_number)                                                             AS sequence_number,
    @clean_account_number(account_number)                                                         AS account_number,

    @cast_to_boolean(is_structured)                                                               AS is_structured,
    @cast_to_boolean(is_civic)                                                                    AS is_civic,

    unit_number::TEXT                                                                             AS unit_number,
    unit_type::TEXT                                                                               AS unit_type,
    civic_number::TEXT                                                                            AS civic_number,
    street::TEXT                                                                                  AS street,
    street_type::TEXT                                                                             AS street_type,
    direction::TEXT                                                                               AS direction,
    city::TEXT                                                                                    AS city,
    province::TEXT                                                                                AS province,
    postal_code::TEXT                                                                             AS postal_code,
    country::TEXT                                                                                 AS country,

    mail_mode::TEXT                                                                               AS mail_mode,
    mail_box::TEXT                                                                                AS mail_box,
    mail_id::TEXT                                                                                 AS mail_id,
    mail_station::TEXT                                                                            AS mail_station,
    station_name::TEXT                                                                            AS station_name,
    station_type::TEXT                                                                            AS station_type,


    line_1::TEXT                                                                                  AS line_1,
    line_2::TEXT                                                                                  AS line_2,
    line_3::TEXT                                                                                  AS line_3,
    line_4::TEXT                                                                                  AS line_4,
    line_5::TEXT                                                                                  AS line_5,
    line_6::TEXT                                                                                  AS line_6,
    line_7::TEXT                                                                                  AS line_7,
    line_8::TEXT                                                                                  AS line_8,


    @{sys_col_ingested_at},
    @{sys_col_data_snapshot_date},
    as_of_date
  FROM src
  WHERE as_of_date BETWEEN @start_ds AND @end_ds
),

deduplicated AS (
  @deduplicate(
    transformed,
    partition_by := (client_code, account_number, sequence_number, as_of_date),
    order_by := ["@{sys_col_ingested_at} DESC"]
  )
)
SELECT * FROM deduplicated;