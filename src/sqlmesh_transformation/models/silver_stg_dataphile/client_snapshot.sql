MODEL (
  name silver_dataphile.clients_snapshot,
  kind SCD_TYPE_2_BY_COLUMN (
    -- time_column @{sys_col_data_snapshot_date},
    unique_key client_code,
    columns scd_hash,
    updated_at_name @{sys_col_data_snapshot_date},
    batch_size 1,
    disable_restatement false,
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
    CASE
      WHEN recipient_type_code = 0 THEN 'Other'
      WHEN recipient_type_code = 1 THEN 'Individual'
      WHEN recipient_type_code = 2 THEN 'Joint'
      WHEN recipient_type_code = 3 THEN 'Corporate'
      WHEN recipient_type_code = 4 THEN 'Association/Trust'
      WHEN recipient_type_code = 5 THEN 'Government'
      WHEN recipient_type_code = 6 THEN 'Charity'
      ELSE ERROR(printf('Unexpected recipient type code: %s', recipient_type_code))
    END                                                                                       AS recipient_type,

    first_name::TEXT                                                                          AS first_name,
    last_name::TEXT                                                                           AS last_name,

    residence_code::TEXT                                                                      AS residence_code,
    employee_code::TEXT                                                                       AS employee_code,
    CASE
      WHEN employee_code = 'N' THEN 'Non-Professional'
      WHEN employee_code = 'Y' THEN 'Employee'
      WHEN employee_code = 'P' THEN 'Non-Employee Professional'
      ELSE ERROR(printf('Unexpected employee code: %s', employee_code))
    END                                                                                       AS is_employee,

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
    CASE
      WHEN nrt_code = 0 THEN 'Resident'
      WHEN nrt_code = 1 THEN 'Non-Resident'
      WHEN nrt_code = 2 THEN 'Non-Resident Exempt'
      WHEN nrt_code = 3 THEN 'Additional'
      WHEN nrt_code = 4 THEN 'Unknown'
      ELSE ERROR(printf('Unexpected NRT code: %s', nrt_code))
    END                                                                                       AS nrt,

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
  WHERE @{sys_col_data_snapshot_date} BETWEEN @start_ds AND @end_ds
),

deduplicated AS (
  @deduplicate(
    transformed,
    partition_by := (client_code, @{sys_col_data_snapshot_date}),
    order_by := ["@{sys_col_ingested_at} DESC"]
  )
),

scd_hash_added AS (
  SELECT
    *,
    md5(
      array_to_string(
        list_transform(
          [UNPACK(COLUMNS(* EXCLUDE (@{sys_col_data_snapshot_date}, @{sys_col_ingested_at}))::VARCHAR)],
          x -> coalesce(x, '_sqlmesh_surrogate_key_null_')
        ),
        '|'
      )
    ) AS scd_hash
  FROM deduplicated
)

SELECT * FROM scd_hash_added;