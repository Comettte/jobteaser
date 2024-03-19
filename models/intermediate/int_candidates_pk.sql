SELECT *,
 CONCAT (user_id," - ",shortlist_id," - ",status_update) as pk_candidates,
 TIMESTAMP_DIFF(timestamp(current_sign_in_at), timestamp(receive_time), HOUR) as timestamp_actions

 FROM {{ ref('int_candidates_deduplicates') }}

