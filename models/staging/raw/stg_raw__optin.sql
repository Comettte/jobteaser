with 

source as (

    select * from {{ source('raw', 'optin') }}

),

renamed as (

    select
        user_id,
        receive_time,
        cause,
        active,
        school_id,
        current_sign_in_at,
        resume_uploaded

    from source

)

select * from renamed
