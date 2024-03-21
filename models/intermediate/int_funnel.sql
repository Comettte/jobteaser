select
    user_id,
    school_id,
    shortlist_id,
    status_update,
    cause,
    receive_time,
    current_sign_in_at,
    rnk
from
    (
        select
            user_id,
            school_id,
            shortlist_id,
            status_update,
            cause,
            receive_time,
            current_sign_in_at,
            rank() over (
                partition by user_id, school_id, shortlist_id order by receive_time desc
            ) as rnk
        from {{ ref("int_candidates_pk") }}
    )
where rnk = 1
