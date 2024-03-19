SELECT *,
 CONCAT (user_id," - ",shortlist_id," - ",status_update) as pk_candidates

 FROM {{ ref('int_candidates_deduplicates') }}

