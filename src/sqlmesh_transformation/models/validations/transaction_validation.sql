MODEL (
    name validations.transaction_validations,
    kind VIEW
);

@DEF('coalesce_to_text', (value) -> (COALESCE(value::TEXT, '<null>')));

WITH latest_data_snapshot_date AS (
    SELECT MAX(@{sys_col_data_snapshot_date}) AS data_snapshot_date
    FROM silver_dataphile.transactions_snapshot
),

WITH reporting_year_start_date AS (
    SELECT DATEFROMPARTS(YEAR(CURRENT_DATE) - 1, 12, 31) AS reporting_year_start_date
),

transactions AS (
    SELECT
        t.*,
    FROM silver_dataphile.transactions_snapshot t
    JOIN reporting_year_start_date r
        ON t.@{sys_col_data_snapshot_date} > r.reporting_year_start_date


    -- WHERE 
    --     a.account_number LIKE '0%'              -- Client accounts
    --     AND a.status = 'ACTIVE'                 -- Active accounts
    --     AND a.account_type != 'COD'             -- COD accounts 
    --     AND a.account_number NOT LIKE '091%'    -- Institutional accounts
    --     AND a.account_number NOT LIKE '093%'    -- Institutional accounts
),

clients AS (
    SELECT c.*,
    (c.recipient_type_code NOT IN (1, 2))                                                    AS is_entity,
    (c.recipient_type_code IN (1, 2))                                                        AS is_not_entity,
    (c.employee_code IN ('Y', 'P'))                                                          AS is_professional,

    FROM silver_dataphile.clients_snapshot c
    JOIN latest_data_snapshot_date l
        ON c.@{sys_col_data_snapshot_date} = l.data_snapshot_date
),

accounts AS (
    SELECT 
        a.*,
    FROM silver_dataphile.accounts_snapshot a
    JOIN latest_data_snapshot_date l
        ON a.@{sys_col_data_snapshot_date} = l.data_snapshot_date
),

transaction_validations AS (
    SELECT
        'TRANSACTION_VALIDATION'::TEXT                                                       AS rule_group,
        r.rule_id::TEXT                                                                      AS rule_id,
        r.severity::TEXT                                                                     AS severity,
        'transaction'::TEXT                                                                  AS entity_type,
        t.journal_reference_number::TEXT                                                     AS entity_id,
        r.message::TEXT                                                                      AS message
    FROM transactions t
    JOIN accounts a
        ON t.account_number = a.account_number
    JOIN clients c
        ON a.client_code = c.client_code
    CROSS JOIN LATERAL (
    VALUES

        /* -------------------------------------------------------------------------------------------------
                Discretionary Flag Validations
        ------------------------------------------------------------------------------------------------- */
        (
            'DISCRETIONARY_FLAG_001',
            'ERROR',
            ( t.currency = 'USD' ),
            ( printf('%s accounts should be marked as discretionary.', a.portfolio_type) )            
        ),
    )
    AS r(rule_id, severity, condition, message)
    WHERE r.condition
)


SELECT * FROM transaction_validations;