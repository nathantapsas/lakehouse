MODEL (
    name validations.account_coding_validations,
    kind VIEW
);

-- @DEF(cad_suffixes, ('A', 'C', 'E', 'G', 'L', 'Q', 'S', 'T', 'X', 'Y'));
-- @DEF(usd_suffixes, ('B', 'D', 'F', 'H', 'M', '6', 'R', 'U'));

WITH latest_data_snapshot_date AS (
    SELECT MAX(@{sys_col_data_snapshot_date}) AS data_snapshot_date
    FROM silver_dataphile.accounts_snapshot
),

accounts AS (
    SELECT
        a.*,
        (SUBSTRING(a.account_number FROM 1 FOR 3))                                      AS account_number_prefix,
        (SUBSTRING(a.account_number FROM LENGTH(a.account_number) FOR 1))               AS account_number_suffix,
        (a.portfolio_type = 'Managed')                                                  AS is_managed,
        (a.portfolio_type = 'SMA')                                                      AS is_sma,
        (a.portfolio_type = 'Fee-Based')                                                AS is_fee_based,
        (a.portfolio_type = 'Commission')                                               AS is_commission,
        (a.sub_type_code IS NOT NULL)                                                   AS is_registered,
        (a.sub_type_code IS NULL)                                                       AS is_not_registered
    FROM silver_dataphile.accounts_snapshot a
    JOIN latest_data_snapshot_date l
        ON a.@{sys_col_data_snapshot_date} = l.data_snapshot_date
    WHERE 
        a.account_number LIKE '0%'              -- Client accounts
        AND a.status = 'ACTIVE'                 -- Active accounts
        AND a.account_type != 'COD'             -- COD accounts 
        AND a.account_number NOT LIKE '091%'    -- Institutional accounts
        AND a.account_number NOT LIKE '093%'    -- Institutional accounts
),

clients AS (
    SELECT c.*
    FROM silver_dataphile.clients_snapshot c
    JOIN latest_data_snapshot_date l
        ON c.@{sys_col_data_snapshot_date} = l.data_snapshot_date
)

SELECT
    'ACCOUNT_CODING'::TEXT                                                               AS rule_group,
    r.rule_id::TEXT                                                                      AS rule_id,
    r.severity::TEXT                                                                     AS severity,
    'account'::TEXT                                                                      AS entity_type,
    a.account_number::TEXT                                                               AS entity_id,
    r.message::TEXT                                                                      AS message
FROM accounts a
JOIN clients c
    ON a.client_code = c.client_code
CROSS JOIN LATERAL (
  VALUES
    -- Discretionary Flag Validations
    (
        'DISCRETIONARY_FLAG_001',
        'ERROR',
        ( (a.is_managed OR a.is_sma) AND NOT a.is_discretionary ),
        ( printf('%s accounts should be marked as discretionary.', a.portfolio_type) )            
    ),
    (
        'DISCRETIONARY_FLAG_002',
        'ERROR',
        ( NOT a.is_managed AND NOT a.is_sma AND a.is_discretionary ),
        ( printf('%s accounts should not be marked as discretionary.', a.portfolio_type) )
    ),
    -- Minimum Commission Check Flag Validation
    (
        'MINIMUM_COMMISSION_CHECK_FLAG_001',
        'ERROR',
        ( (a.is_sma OR a.is_fee_based OR a.is_managed) AND a.minimum_commission_check ),
        ( printf('%s accounts should not have minimum commission check enabled.', a.portfolio_type) )
    ),
    (
        'MINIMUM_COMMISSION_CHECK_FLAG_002',
        'ERROR',
        ( a.is_commission AND NOT a.minimum_commission_check ),
        'Commission accounts should have minimum commission check enabled.'
    ),
    -- Type Validation
    (
        'TYPE_001',
        'WARNING',
        ( a.account_type_code = '4' ),
        'Account is in client name.'
    ),
    -- Residence Code Validation
    (
        'RESIDENCE_CODE_001',
        'ERROR',
        ( c.residence_code != a.residence_code ),
         'Account residence code does not match client residence code.'
    ),
    -- Suffix Validation
    (
        'SUFFIX_001',
        'ERROR',
        NOT list_contains(
            CASE (a.account_type_code::TEXT || '_' || a.currency)
                -- TYPE '0' (Client Name?)
                WHEN '0_CAD' THEN ['A','I','J','K','N','O','P','Q','S','T','V','W','X','Y','Z']
                WHEN '0_USD' THEN ['B','6','R','U']
                
                -- TYPE '1'
                WHEN '1_CAD' THEN ['C']
                WHEN '1_USD' THEN ['D']
                
                -- TYPE '2'
                WHEN '2_CAD' THEN ['E', 'L']
                WHEN '2_USD' THEN ['F', 'M']
                
                -- TYPE '3'
                WHEN '3_CAD' THEN ['G']
                WHEN '3_USD' THEN ['H']
                
                -- If account type is unknown or not in map, return empty list (fail)
                ELSE []
            END,
            a.account_number_suffix
        ),
        printf('Account number suffix "%s" is not valid for "%s" accounts with currency "%s".', 
               coalesce(a.account_number_suffix, '<null>'), 
               coalesce(a.account_type, '<null>'), 
               coalesce(a.currency, '<null>')
        )
    ),
    (
        'SUFFIX_002',
        'ERROR',
        ( 
            NOT list_contains(
                CASE (COALESCE(a.sub_type_code, 'NULL') || '_' || a.currency)
                    -- Format: WHEN 'SubType_Currency' THEN ['Valid', 'Suffixes']
                    WHEN 'NULL_CAD' THEN ['A', 'C', 'E', 'G', 'L', 'N'] WHEN 'NULL_USD' THEN ['B', 'D', 'F', 'H', 'M']
                    WHEN 'I_CAD'    THEN ['I'] WHEN 'I_USD'    THEN ['R']
                    WHEN 'J_CAD'    THEN ['J'] WHEN 'J_USD'    THEN ['']
                    WHEN 'L_CAD'    THEN ['K'] WHEN 'L_USD'    THEN ['U']
                    WHEN 'C_CAD'    THEN ['O'] WHEN 'C_USD'    THEN ['R']
                    WHEN 'P_CAD'    THEN ['P'] WHEN 'P_USD'    THEN ['R']
                    WHEN 'T_CAD'    THEN ['Q'] WHEN 'T_USD'    THEN ['6']
                    WHEN 'N_CAD'    THEN ['T'] WHEN 'N_USD'    THEN ['R']
                    WHEN 'X_CAD'    THEN ['X'] WHEN 'X_USD'    THEN ['R']
                    WHEN 'R_CAD'    THEN ['S', 'U'] WHEN 'R_USD'    THEN ['U']
                    WHEN 'S_CAD'    THEN ['Y'] WHEN 'S_USD'    THEN ['U']
                    WHEN 'G_CAD'    THEN ['V'] WHEN 'G_USD'    THEN ['']
                    WHEN 'D_CAD'    THEN ['W'] WHEN 'D_USD'    THEN ['']
                    WHEN 'V_CAD'    THEN ['Z'] WHEN 'V_USD'    THEN ['']
                    WHEN 'E_CAD'    THEN ['Z'] WHEN 'E_USD'    THEN ['']
                    ELSE [] -- If key combination not found, return empty list (automatic violation)
                END,
                a.account_number_suffix -- The value to look for:
            )
        ),
        printf('Account number suffix "%s" is not valid "%s" accounts with currency "%s".', coalesce(a.account_number_suffix, '<null>'), coalesce(a.sub_type, '<null>'), coalesce(a.currency, '<null>') )
    )

) AS r(rule_id, severity, is_violation, message)
WHERE r.is_violation;
