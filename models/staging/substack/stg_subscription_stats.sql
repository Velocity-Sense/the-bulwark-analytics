-- models/staging/substack/stg_subscription_stats.sql

with

final as (

    select * from {{ source('substack', 'subscription_stats') }}

)

select * from final