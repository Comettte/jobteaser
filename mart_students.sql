
WITH shortlists as(
SELECT  
user_id,
COUNT (shortlist_id) as nb_shortlists,
SUM(CASE WHEN status_update = 'awaiting' Then 1 ELSE 0 END) as awaiting,
SUM(CASE WHEN status_update = 'not interested'THEN 1 ELSE 0 END) as not_interested,
SUM(CASE WHEN status_update = 'interested'THEN 1 ELSE 0 END) as interested,
SUM(CASE WHEN status_update = 'approved'THEN 1 ELSE 0 END) as approved,
SUM(CASE WHEN status_update = 'declined'THEN 1 ELSE 0 END)  as declined,
FROM `wise-analyst-417610.dbt__intermediate.int_funnel`
GROUP BY user_id)

SELECT 
students.*,
IFNULL (nb_shortlists,0) as nb_shortlists,
IFNULL (awaiting,0) as awaiting,
IFNULL(not_interested,0) as not_interested,
IFNULL(interested,0) as interested,
IFNULL(approved,0) as approved,
IFNULL(declined,0) as declined
FROM `wise-analyst-417610.dbt__intermediate.int_students` as students
LEFT JOIN shortlists
ON students.user_id = shortlists.user_id