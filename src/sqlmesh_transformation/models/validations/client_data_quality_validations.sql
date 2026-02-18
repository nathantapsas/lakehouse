MODEL (
    name validations.client_data_quality_validations,
    kind FULL,
);

@DEF('coalesce_to_text', (value) -> (COALESCE(value::TEXT, '<null>'))
);

WITH latest_as_of_date AS (
    SELECT MAX(__data_as_of_date) AS latest_date
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
    JOIN latest_as_of_date l
        ON a.__data_as_of_date = l.latest_date
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
    (c.recipient_type_code IN (1, 2))                                                        AS is_not_entity,
    (c.employee_code IN ('Y', 'P'))                                                          AS is_professional

    FROM silver_dataphile.clients_snapshot c
    JOIN latest_as_of_date l
        ON c.__data_as_of_date = l.latest_date
    WHERE EXISTS (
        SELECT 1 FROM accounts a
        WHERE a.client_code = c.client_code
    )
),

associated_parties AS (
    SELECT 
        ap.*,
        (ap.sequence_number = 1)                                                             AS is_primary_processing_record
    FROM silver_dataphile.associated_parties_snapshot ap
    JOIN latest_as_of_date l
        ON ap.__data_as_of_date = l.latest_date
    WHERE 
        ap.status = 'ACTIVE'
        AND EXISTS (
            SELECT 1 FROM clients c
            WHERE c.client_code = ap.client_code
        )
),

addresses AS (
    SELECT
        ad.*,
        (NOT ad.is_structured)                                                               AS is_freeform,
        (ad.is_structured AND NOT ad.is_civic)                                               AS is_rural,
        (ad.country = 'CAN')                                                                 AS is_canadian,
        (ad.country = 'USA')                                                                 AS is_us
    FROM silver_dataphile.addresses_snapshot ad
    JOIN latest_as_of_date l
        ON ad.__data_as_of_date = l.latest_date
    WHERE EXISTS (
        SELECT 1 FROM clients c
        WHERE c.client_code = ad.client_code
    )
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
                Class Validations
        ------------------------------------------------------------------------------------------------- */
        -- (
        --     'CLASS_001',
        --     'ERROR',
        --     ( a.account_type_code = '4' ),
        --     'Account is in client name.'
        -- ),
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
                    WHEN '4_CAD' THEN ['A'] WHEN '4_USD' THEN ['B']                                 -- TYPE '4' 
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
        (
            'ENTITY_004',
            'ERROR',
            ( c.is_entity AND (
                SELECT COUNT(*) FROM associated_parties ap
                WHERE ap.client_code = c.client_code
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
            'Non-entity clients with spouse name should have spouse SIN and spouse birth date, but found ' ||
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
),

associated_party_validations AS (
    SELECT
        'ASSOCIATED_PARTY_CODING'::TEXT                                                      AS rule_group,
        r.rule_id::TEXT                                                                      AS rule_id,
        r.severity::TEXT                                                                     AS severity,
        'associated_party'::TEXT                                                             AS entity_type,
        c.client_code::TEXT ||  ' - ' || ap.sequence_number::TEXT                            AS entity_id,
        r.message::TEXT                                                                      AS message
    FROM associated_parties ap
    JOIN clients c
        ON ap.client_code = c.client_code
    CROSS JOIN LATERAL (
    VALUES
        /* -------------------------------------------------------------------------------------------------
                Title Validations
        ------------------------------------------------------------------------------------------------- */
        (
            'TITLE_001',
            'ERROR',
            ( ap.title IS NULL ),
            'Associated party is missing a title.'
        ),
        /* -------------------------------------------------------------------------------------------------
                Missing Data Validations
        ------------------------------------------------------------------------------------------------- */
        (
            'MISSING_DATA_001',
            'ERROR',
            ( ap.citizenship IS NULL OR ap.residence_code IS NULL ),
            'Associated party is missing ' ||
            concat_ws(' and ',
                CASE WHEN ap.citizenship IS NULL THEN 'citizenship' END,
                CASE WHEN ap.residence_code IS NULL THEN 'residence code' END
            ) || '.'
        ),
        /* -------------------------------------------------------------------------------------------------
                SIN Validations
        ------------------------------------------------------------------------------------------------- */
        (
            'SIN_001',
            'WARNING',
            ( ap.sin LIKE '9%' ),
            'Associated party is using a temporary SIN.'
        ),
        /* -------------------------------------------------------------------------------------------------
                Primary Processing Record Validations
        ------------------------------------------------------------------------------------------------- */
        (
            'PRIMARY_PROCESSING_RECORD_001',
            'ERROR',
            ( ap.is_primary_processing_record and ap.sin IS NOT NULL and c.sin IS NOT NULL and ap.sin != c.sin ),
            'Primary associated party SIN does not match client SIN.'
        ),
        (
            'PRIMARY_PROCESSING_RECORD_002',
            'ERROR',
            ( ap.is_primary_processing_record and ap.ssn IS NOT NULL and c.ssn IS NOT NULL and ap.ssn != c.ssn ),
            printf('Primary associated party SSN %s does not match client SSN %s.', ap.ssn, c.ssn)
        ),
        (
            'PRIMARY_PROCESSING_RECORD_003',
            'ERROR',
            ( ap.is_primary_processing_record and ap.citizenship IS NOT NULL and c.citizenship IS NOT NULL and ap.citizenship != c.citizenship ),
            printf('Primary associated party citizenship %s does not match client citizenship %s.', ap.citizenship, c.citizenship)
        ),
        (
            'PRIMARY_PROCESSING_RECORD_004',
            'ERROR',
            ( ap.is_primary_processing_record and ap.residence_code IS NOT NULL and c.residence_code IS NOT NULL and ap.residence_code != c.residence_code ),
            printf('Primary associated party residence code %s does not match client residence code %s.', ap.residence_code, c.residence_code)
        ),
        (
            'PRIMARY_PROCESSING_RECORD_005',
            'ERROR',
            ( ap.is_primary_processing_record and ap.birth_date IS NOT NULL and c.birth_date IS NOT NULL and ap.birth_date != c.birth_date ),
            printf('Primary associated party birth date %s does not match client birth date %s.', ap.birth_date, c.birth_date)
        )
    ) AS r(rule_id, severity, is_violation, message)
    WHERE r.is_violation
),

address_validations AS (
    SELECT
        'ADDRESS_CODING'::TEXT                                                               AS rule_group,
        r.rule_id::TEXT                                                                      AS rule_id,
        r.severity::TEXT                                                                     AS severity,
        'address'::TEXT                                                                      AS entity_type,
        c.client_code::TEXT ||  ' - ' || ad.sequence_number::TEXT                            AS entity_id,
        r.message::TEXT                                                                      AS message
    FROM addresses ad
    JOIN clients c
        ON ad.client_code = c.client_code
    CROSS JOIN LATERAL (
    VALUES
        /* -------------------------------------------------------------------------------------------------
                Canadian / USA Address Validations
        ------------------------------------------------------------------------------------------------- */
        (
            'CAN_US_001',
            'ERROR',
            ( (ad.is_canadian OR ad.is_us) AND ad.is_freeform ),
            'Canadian and US addresses should be structured, but address is freeform.'
        ),
        (
            'CAN_US_002',
            'ERROR',
            ( (ad.is_canadian OR ad.is_us) AND (ad.city IS NULL OR ad.province IS NULL OR ad.postal_code IS NULL) ),
            'Canadian and US addresses should have a ' ||
            concat_ws(
                ' and ',
                CASE WHEN ad.city IS NULL THEN 'city' END,
                CASE WHEN ad.province IS NULL THEN 'province' END,
                CASE WHEN ad.postal_code IS NULL THEN 'postal code' END
            ) || ' specified.'
        )
    ) AS r(rule_id, severity, is_violation, message)
    WHERE r.is_violation
)

SELECT * FROM account_validations
UNION ALL
SELECT * FROM client_validations
UNION ALL 
SELECT * FROM associated_party_validations
UNION ALL
SELECT * FROM address_validations
