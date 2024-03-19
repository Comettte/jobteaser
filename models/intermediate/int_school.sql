WITH career_center AS (
    SELECT 
        school_id,
        is_cc,
        intranet_school_id,
        jt_country,
        jt_school_type,
        CASE 
            WHEN is_cc THEN 'jobteaser'
            WHEN intranet_school_id IS NOT NULL THEN 'intern'
            ELSE 'none'
        END AS career_center_type
    FROM {{ ref('stg_raw__dim_schools') }}
        
)

SELECT 
    school_id,
    CASE 
        WHEN jt_country IN ('Czechia', 'Czech Republic') THEN 'Czechia'
        ELSE jt_country
    END AS jt_country,
    jt_school_type,
    career_center_type
FROM 
    career_center