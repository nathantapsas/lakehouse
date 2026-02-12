MODEL (
  name silver_dataphile.clients_snapshot,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column @{sys_col_data_snapshot_date},
    batch_size 1,
  ),
);

WITH transformed AS (
  SELECT
    @cast_to_integer(client_code)                                                             AS client_code,
    client_id::TEXT                                                                           AS client_id,

    status_code::TEXT                                                                         AS status_code,
    @clean_status(status_code)                                                                AS status,
    ia_code::TEXT                                                                             AS ia_code,
    @cast_to_integer(recipient_type_code)                                                     AS recipient_type_code,

    first_name::TEXT                                                                          AS first_name,
    last_name::TEXT                                                                           AS last_name,

    residence_code::TEXT                                                                      AS residence_code,
    employee_code::TEXT                                                                       AS employee_code,

    monitor_code::TEXT                                                                        AS monitor_code,
    citizenship::TEXT                                                                         AS citizenship,
    @cast_to_boolean(is_us_person)                                                            AS is_us_person,
    @cast_to_date(birth_date, format := '%m/%d/%Y')                                           AS birth_date,
    @cast_to_date(deceased_date, format := '%m/%d/%Y')                                        AS deceased_date,

    sin::TEXT                                                                                 AS sin,
    ssn::TEXT                                                                                 AS ssn,
    corporate_id_type::TEXT                                                                   AS corporate_id_type,
    corporate_id::TEXT                                                                        AS corporate_id,

    spouse_name::TEXT                                                                         AS spouse_name,
    spouse_sin::TEXT                                                                          AS spouse_sin,
    @cast_to_date(spouse_birth_date, format := '%m/%d/%Y')                                    AS spouse_birth_date,

    @cast_to_integer(nrt_code)                                                                AS nrt_code,

    @cast_to_integer(household_id)                                                            AS household_id,

    verification_id::TEXT                                                                     AS verification_id,
    @cast_to_boolean(consolidate)                                                             AS consolidate,

    branch_code::TEXT                                                                         AS branch_code,

    @cast_to_date(open_date, format := '%m/%d/%y', prefer_past := True)                       AS open_date,
    @cast_to_date(close_date, format := '%m/%d/%y', prefer_past := True)                      AS close_date,

    @cast_to_boolean(is_dormant)                                                              AS is_dormant,

    @{sys_col_ingested_at},
    @{sys_col_data_snapshot_date}
  FROM bronze.clients
),

deduplicated AS (
  @deduplicate(
    transformed,
    partition_by := (client_code, @{sys_col_data_snapshot_date}),
    order_by := ["@{sys_col_ingested_at} DESC"]
  )
)
SELECT * FROM deduplicated