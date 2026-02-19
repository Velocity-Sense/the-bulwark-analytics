-- models/intermediate/int_churned_subscriptions.sql

with

base as (

    select

        ss.user_id,
        cs.* exclude (user_id)

    from {{ ref('stg_substack__subscription_stats') }} ss
    left join {{ ref('stg_substack__churned_subscriptions') }} cs
        on cs.user_id = ss.user_id

    union all

    -- Include any users who have churned and not returned so
    -- wouldn't be in the subscription_stats table
    select

        cs.*

    from {{ ref('stg_substack__churned_subscriptions') }} cs

),

final as (

    select

        *
    
    from base
    where churn_at is not null

)

select * from final