SELECT DISTINCT *
FROM {{ ref('stg_raw__candidate_status_update') }}