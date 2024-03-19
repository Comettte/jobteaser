select
    school_id,
    jt_country,
    career_center_type,

    case
        when
            jt_country in (
                'Germany',
                'Spain',
                'Belgium',
                'France',
                'Austria',
                'Netherlands',
                'Estonia',
                'Switzerland',
                'Denmark',
                'United Kingdom',
                'Norway',
                'Ireland',
                'Sweden',
                'Portugal',
                'Finland',
                'Italy',
                'Luxembourg',
                'Russian Federation',
                'Romania',
                'Cyprus',
                'Poland',
                'Greece',
                'Andorra',
                'Slovenia',
                'Serbia',
                'Croatia',
                'Hungary',
                'Monaco',
                'Gibraltar',
                'Czechia'
            )
        then 'Europe'
        when jt_country in ('United Arab Emirates', 'Lebanon')
        then 'Middle-East'
        when
            jt_country
            in ('United States', 'Nicaragua', 'Colombia', 'Puerto Rico', 'Bahamas')
        then 'America'
        when jt_country in ('Australia', 'Nauru')
        then 'Oceania'
        when jt_country in ('Ghana', "Côte D'Ivoire")
        then 'Africa'
    end as jt_school_zone,

    case
        when jt_school_type = 1
        then 'Universities of applied Science'
        when jt_school_type = 2
        then 'Business Schools / Business Universities'
        when jt_school_type = 3
        then 'Engineering / Technical Universities'
        when jt_school_type = 4
        then 'Institute of Political Studies'
        when jt_school_type = 5
        then 'Part-Time UAS'
        when jt_school_type = 6
        then 'Alumni / Student Associations'
        when jt_school_type = 7
        then 'Universities'
        when jt_school_type = 8
        then 'BTS / Bachelor / IUT'
    end as jt_school_category

from {{ ref("int_school") }}
