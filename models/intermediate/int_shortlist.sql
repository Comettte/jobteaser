with
    shortlist_schools as (
        select status.*, 
        jt_country
        from {{ ref("int_candidates_deduplicates") }} as status
        left join
            {{ ref("int_school_category_zone") }} as schools
            on schools.school_id = status.school_id
    ),
shortlist as (
select
    shortlist_id,
    count(distinct user_id) as nb_students,
    count(distinct shortlist_schools.school_id) as nb_schools,
    count(distinct jt_country) as nb_countries

from shortlist_schools
group by shortlist_id),

statuses as (

SELECT  
shortlist_id,
SUM(CASE WHEN status_update = 'awaiting' Then 1 ELSE 0 END) as awaiting,
SUM(CASE WHEN status_update = 'not interested'THEN 1 ELSE 0 END) as not_interested,
SUM(CASE WHEN status_update = 'interested'THEN 1 ELSE 0 END) as interested,
SUM(CASE WHEN status_update = 'approved'THEN 1 ELSE 0 END) as approved,
SUM(CASE WHEN status_update = 'declined'THEN 1 ELSE 0 END)  as declined,
FROM `wise-analyst-417610.dbt__intermediate.int_funnel`
GROUP by shortlist_id
)
SELECT
shortlist.*,
awaiting,
not_interested,
interested,
approved,
declined
FROM shortlist
LEFT JOIN statuses
ON shortlist.shortlist_id = statuses.shortlist_id