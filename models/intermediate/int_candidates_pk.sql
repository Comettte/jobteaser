WITH deduplicates AS
(SELECT DISTINCT *
FROM {{ ref('stg_raw__candidate_status_update') }})

SELECT *,

 CONCAT (user_id," - ",shortlist_id," - ",status_update) as pk_candidates,
 TIMESTAMP_DIFF(timestamp(current_sign_in_at), timestamp(receive_time), HOUR) as timestamp_actions

 FROM deduplicates

