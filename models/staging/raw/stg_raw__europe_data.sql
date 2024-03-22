with 

source as (

    select * from {{ source('raw', 'europe_data') }}

),

renamed as (

    select
        freq,
        time_format,
        time_period,
        sex,
        field,
        isced97,
        geo,
        obs_status,
        unit,
        obs_value,
        freq_desc,
        time_format_desc,
        time_period_desc,
        obs_status_desc,
        unit_desc,
        sex_desc,
        field_desc,
        isced97_desc,
        geo_desc

    from source

)

select * from renamed
