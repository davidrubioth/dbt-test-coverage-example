with
    height as (
        select 
            created_date,
            user_id,
            height,
            measurement_unit,
            row_number() over (partition by user_id order by created_date desc) as row_number
        from {{ ref('stg_gym_app__height') }}

    ),
    latest_height as (
        select 
            created_date,
            user_id,
            height,
            measurement_unit
        from height
        where row_number = 1
    ),
    weight as (
        select 
            created_date,
            weight,
            user_id,
            measurement_unit
        from {{ ref('stg_gym_app__weight') }}
    )

select
    GREATEST(latest_height.created_date, weight.created_date) as created_date,
    height,
    weight,
    weight.user_id
from weight 
inner join latest_height 
on weight.user_id = latest_height.user_id