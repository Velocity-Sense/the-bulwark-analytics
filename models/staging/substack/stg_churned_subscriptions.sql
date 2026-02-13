-- models/staging/substack/stg_churned_subscriptions.sql

with

final as (

    select * from {{ source('substack', 'churned_subscriptions') }}

)

select * from final