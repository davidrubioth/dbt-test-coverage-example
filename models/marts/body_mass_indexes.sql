with
    weights_with_latest_height as (
        select 
            created_date,
            height,
            weight,
            user_id
        from {{ ref('int_weight_measurements_with_latest_height') }}
    ),
    body_mass_indexes as (
        select
            created_date,
            user_id,
            weight,
            height,
            round((weight::numeric / (height::numeric / 100) ^ 2)::numeric, 1) as bmi,
            current_timestamp as processed_at
        from weights_with_latest_height
    )

select 
    created_date,   
    user_id,
    weight,
    height,
    bmi,
    processed_at
from body_mass_indexes