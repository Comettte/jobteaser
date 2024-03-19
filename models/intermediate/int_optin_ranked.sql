with
    optin_distinct as (
    select 
    distinct *
    from {{ ref("stg_raw__optin") }})
    select
            user_id,
            school_id,
            cause,
            active,
            resume_uploaded,
            receive_time,
            current_sign_in_at,
            rnk
        from
            (
                select
                    user_id,
                    school_id,
                    cause,
                    active,
                    resume_uploaded,
                    receive_time,
                    current_sign_in_at,
                    rank() over (
                        partition by user_id
                        order by receive_time desc, active desc, resume_uploaded desc
                    ) as rnk
                from optin_distinct
            )
        where rnk = 1
