SELECT  
user_id,
school_id,
shortlist_id,
status_update,
cause,
receive_time,
current_sign_in_at,
rnk
FROM (
  SELECT 
  user_id,
  school_id,
  shortlist_id,
  status_update,
  cause,
  receive_time,
  current_sign_in_at,
  RANK() OVER(PARTITION BY user_id, school_id, shortlist_id ORDER BY receive_time DESC) as rnk
  FROM from {{ ref("int_candidates_deduplicates") }}
)
WHERE rnk=1
