with
    shortlist_schools as (
        select status.*, 
        jt_country
        from {{ ref("stg_raw__candidate_status_update") }} as status
        left join
            {{ ref("stg_raw__dim_schools") }} as schools
            on schools.school_id = status.school_id
    )

select
    shortlist_id,
    count(distinct user_id) as nb_students,
    count(distinct shortlist_schools.school_id) as nb_schools,
    count(distinct jt_country) as nb_countries

from shortlist_schools
group by shortlist_id
