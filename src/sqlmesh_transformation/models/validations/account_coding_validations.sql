MODEL (
    name validations.account_coding_validations,
    kind VIEW
);

@DEF(
    'coalesce_to_text',
    (value) -> (
        COALESCE(value::TEXT, '<null>')
    )
);

WITH latest_data_snapshot_date AS (
    SELECT MAX(@{sys_col_data_snapshot_date}) AS data_snapshot_date
    FROM silver_dataphile.accounts_snapshot
),

accounts AS (
    SELECT
        a.*,
        (SUBSTRING(a.account_number FROM 1 FOR 3))                                          AS account_number_prefix,
        (SUBSTRING(a.account_number FROM LENGTH(a.account_number) FOR 1))                   AS account_number_suffix,
        (a.portfolio_type = 'Managed')                                                      AS is_managed,
        (a.portfolio_type = 'SMA')                                                          AS is_sma,
        (a.portfolio_type = 'Fee-Based')                                                    AS is_fee_based,
        (a.portfolio_type = 'Commission')                                                   AS is_commission,
        (a.sub_type_code IS NOT NULL)                                                       AS is_registered,
        (a.sub_type_code IS NULL)                                                           AS is_not_registered
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
    SELECT c.*,
    (c.recipient_type_code NOT IN (1, 2))                                                    AS is_entity,
    (c.recipient_type_code IN (1, 2))                                                        AS is_not_entity

    FROM silver_dataphile.clients_snapshot c
    JOIN latest_data_snapshot_date l
        ON c.@{sys_col_data_snapshot_date} = l.data_snapshot_date
),

account_validations AS (
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

        /* -------------------------------------------------------------------------------------------------
                Discretionary Flag Validations
        ------------------------------------------------------------------------------------------------- */
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
        /* -------------------------------------------------------------------------------------------------
                Minimum Commission Check Validations
        ------------------------------------------------------------------------------------------------- */
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
        /* -------------------------------------------------------------------------------------------------
                Type Validations
        ------------------------------------------------------------------------------------------------- */
        (
            'TYPE_001',
            'WARNING',
            ( a.account_type_code = '4' ),
            'Account is in client name.'
        ),
        /* -------------------------------------------------------------------------------------------------
                Residence Code Validations
        ------------------------------------------------------------------------------------------------- */
        (
            'RESIDENCE_CODE_001',
            'ERROR',
            ( c.residence_code != a.residence_code ),
            printf('Account residence code %s does not match client residence code %s.', a.residence_code, c.residence_code)
        ),
        /* -------------------------------------------------------------------------------------------------
                Suffix Validations
        ------------------------------------------------------------------------------------------------- */
        (
            'SUFFIX_001',
            'ERROR',
            NOT list_contains(
                CASE (a.account_type_code::TEXT || '_' || a.currency)
                    -- TYPE '0' (Client Name?)
                    WHEN '0_CAD' THEN ['A','I','J','K','N','O','P','Q','S','T','V','W','X','Y','Z'] -- TYPE '0' 
                    WHEN '0_USD' THEN ['B','6','R','U']                                             -- TYPE '0'
                    WHEN '1_CAD' THEN ['C'] WHEN '1_USD' THEN ['D']                                 -- TYPE '1'
                    WHEN '2_CAD' THEN ['E', 'L'] WHEN '2_USD' THEN ['F', 'M']                       -- TYPE '2'
                    WHEN '3_CAD' THEN ['G'] WHEN '3_USD' THEN ['H']                                 -- TYPE '3'
                    -- If account type is unknown or not in map, return empty list (fail)
                    ELSE []
                END,
                a.account_number_suffix
            ),
            printf('Account number suffix "%s" is not valid for %s %s accounts.', coalesce(a.account_number_suffix, '<null>'), a.currency, a.account_type)
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
            printf('Account number suffix "%s" is not valid for %s %s accounts.', @coalesce_to_text(a.account_number_suffix), a.currency, a.sub_type)
        ),
        /* -------------------------------------------------------------------------------------------------
                Options Approval Level Code Validations
        ------------------------------------------------------------------------------------------------- */
        (
            'OPTIONS_APPROVAL_LEVEL_CODE_001',
            'ERROR',
            ( a.is_registered AND a.option_approval_level_code > 2 ),
            printf('Registered accounts should have options approval level of 2 or lower, but account has level %s.', @coalesce_to_text(a.option_approval_level_code))
        ),
        /* -------------------------------------------------------------------------------------------------
                Open Date Validations
        ------------------------------------------------------------------------------------------------- */
        (
            'OPEN_DATE_001',
            'ERROR',
            ( a.opened_date IS NULL ),
            ('Account opened date is blank.')
        ),
        (
            'OPEN_DATE_002',
            'ERROR',
            ( a.opened_date > current_date ),
            printf('Account opened date %s is in the future.', @coalesce_to_text(a.opened_date))
        ),
        (
            'OPEN_DATE_003',
            'WARNING',
            ( a.opened_date < c.birth_date ),
            printf('Account opened date %s is before client birth date %s.', @coalesce_to_text(a.opened_date), @coalesce_to_text(c.birth_date))
        ),
        (
            'OPEN_DATE_004',
            'WARNING',
            ( a.opened_date < c.birth_date + INTERVAL '18 years' ),
            printf('Account opened date %s is before client turned 18 years old on %s.', @coalesce_to_text(a.opened_date), @coalesce_to_text((c.birth_date + INTERVAL '18 years')::DATE))
        ),
        /* -------------------------------------------------------------------------------------------------
                IA Code Validations
        ------------------------------------------------------------------------------------------------- */
        (
            'IA_CODE_001',
            'ERROR',
            ( a.ia_code != c.ia_code ),
            printf('Account IA code %s does not match client IA code %s.', @coalesce_to_text(a.ia_code), @coalesce_to_text(c.ia_code))
        )
    ) AS r(rule_id, severity, is_violation, message)
    WHERE r.is_violation
),

client_validations AS (
    SELECT
        'CLIENT_CODING'::TEXT                                                                AS rule_group,
        r.rule_id::TEXT                                                                      AS rule_id,
        r.severity::TEXT                                                                     AS severity,
        'client'::TEXT                                                                       AS entity_type,
        c.client_code::TEXT                                                                  AS entity_id,
        r.message::TEXT                                                                      AS message
    FROM clients c
    CROSS JOIN LATERAL (
    VALUES
        /* -------------------------------------------------------------------------------------------------
                Missing Data Validations
        ------------------------------------------------------------------------------------------------- */
        (
            'MISSING_DATA_001',
            'WARNING',
            ( c.sin IS NULL and c.ssn IS NULL and c.corporate_id IS NULL ),
            'Client has no SIN, SSN, or corporate ID.'
        ),
        /* -------------------------------------------------------------------------------------------------
                Entity Validations
        ------------------------------------------------------------------------------------------------- */
        (
            'ENTITY_001',
            'ERROR',
            ( c.is_entity AND (c.spouse_name IS NOT NULL OR c.spouse_sin IS NOT NULL OR c.spouse_birth_date IS NOT NULL) ),
            'Entity clients should not have spouse information, but found ' || 
            concat_ws(
                ', ',
                CASE WHEN c.spouse_name IS NOT NULL THEN 'spouse_name: ' || c.spouse_name END,
                CASE WHEN c.spouse_sin IS NOT NULL THEN 'spouse_sin: ' || c.spouse_sin END,
                CASE WHEN c.spouse_birth_date IS NOT NULL THEN 'spouse_birth_date: ' || c.spouse_birth_date END
            ) || '.'
        ),
        (
            'ENTITY_002',
            'ERROR',
            ( c.is_entity AND c.birth_date IS NOT NULL ),
            printf('Entity clients should not have birth date, but found birth_date: %s.', (c.birth_date))
        ),
        (
            'ENTITY_003',
            'ERROR',
            ( c.is_entity AND c.corporate_id IS NULL ),
            'Entity clients should have a corporate ID.'
        ),
        -- TODO: FIX we need status code
        (
            'ENTITY_004',
            'ERROR',
            ( c.is_entity AND (
                SELECT COUNT(*) FROM silver_dataphile.associated_parties_snapshot ap
                WHERE ap.client_code = c.client_code
                AND ap.@{sys_col_data_snapshot_date} = c.@{sys_col_data_snapshot_date}
            ) < 2 ),
            'Entity clients should have at least 2 associated parties.'
        ),
        /* -------------------------------------------------------------------------------------------------
                Non Entity Validations
        ------------------------------------------------------------------------------------------------- */
        (
            'NON_ENTITY_001',
            'WARNING',
            ( c.is_not_entity AND c.spouse_name IS NOT NULL AND (c.spouse_sin IS NULL OR c.spouse_birth_date IS NULL) ),
            'Entity clients should not have spouse information, but found ' ||
            concat_ws(
                ', ',
                CASE WHEN c.spouse_name IS NOT NULL THEN 'spouse_name: ' || c.spouse_name END,
                CASE WHEN c.spouse_sin IS NOT NULL THEN 'spouse_sin: ' || c.spouse_sin END,
                CASE WHEN c.spouse_birth_date IS NOT NULL THEN 'spouse_birth_date: ' || c.spouse_birth_date END
            ) || '.'        
        ),
        (
            'NON_ENTITY_002',
            'WARNING',
            ( c.is_not_entity AND c.spouse_sin IS NOT NULL AND (c.spouse_name IS NULL OR c.spouse_birth_date IS NULL) ),
            'Non-entity clients with spouse SIN should have spouse name and spouse birth date, but found ' ||
            concat_ws(
                ', ',
                CASE WHEN c.spouse_name IS NOT NULL THEN 'spouse_name: ' || c.spouse_name END,
                CASE WHEN c.spouse_sin IS NOT NULL THEN 'spouse_sin: ' || c.spouse_sin END,
                CASE WHEN c.spouse_birth_date IS NOT NULL THEN 'spouse_birth_date: ' || c.spouse_birth_date END
            ) || '.'
        ),
        (
            'NON_ENTITY_003',
            'WARNING',
            ( c.is_not_entity AND c.spouse_birth_date IS NOT NULL AND (c.spouse_name IS NULL OR c.spouse_sin IS NULL) ),
            'Non-entity clients with spouse birth date should have spouse name and spouse SIN, but found ' ||
            concat_ws(
                ', ',
                CASE WHEN c.spouse_name IS NOT NULL THEN 'spouse_name: ' || c.spouse_name END,
                CASE WHEN c.spouse_sin IS NOT NULL THEN 'spouse_sin: ' || c.spouse_sin END,
                CASE WHEN c.spouse_birth_date IS NOT NULL THEN 'spouse_birth_date: ' || c.spouse_birth_date END
            ) || '.'
        ),
        (
            'NON_ENTITY_004',
            'ERROR',
            ( c.is_not_entity AND c.corporate_id IS NOT NULL ),
            printf('Non-entity clients should not have a corporate ID, but found corporate_id: %s.', (c.corporate_id))
        ),
        (
            'NON_ENTITY_005',
            'ERROR',
            ( c.is_not_entity AND c.birth_date IS NULL ),
            'Non-entity clients should have a birth date.'
        ),
        (
            'NON_ENTITY_006',
            'ERROR',
            ( c.is_not_entity AND c.birth_date > current_date ),
            printf('Non-entity client birth date %s is in the future.', (c.birth_date))
        ),
        (
            'NON_ENTITY_007',
            'WARNING',
            ( c.is_not_entity AND c.birth_date < current_date - INTERVAL '150 years' ),
            printf('Non-entity client birth date %s is more than 150 years ago.', (c.birth_date))
        ),
    ) AS r(rule_id, severity, is_violation, message)
    WHERE r.is_violation
)

SELECT * FROM account_validations
UNION ALL
SELECT * FROM client_validations

