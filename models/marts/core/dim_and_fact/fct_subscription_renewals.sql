-- models/marts/core/dim_and_fact/fct_subscription_renewals.sql

with 

final as (

    select * from {{ ref('int_subscription_renewals') }}

)

select * from final