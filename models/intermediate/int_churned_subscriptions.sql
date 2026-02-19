-- models/intermediate/int_churned_subscriptions.sql

with 

min_churn as (
    
    select 
     
        min(last_seen_dt)::date as min_churn_dt
    
    from {{ ref('stg_substack__churned_subscriptions') }}

),

final as (

    select

        convert_timezone('US/Pacific', ss.subscription_created_at)::date as start_dt,
        ss.user_id,
        ss.free_attribution,
        min(cs.last_seen_dt) as churn_dt

    from {{ ref('stg_substack__subscription_stats') }} ss
    left join {{ ref('stg_substack__churned_subscriptions') }} cs
        on cs.user_id = ss.user_id
    cross join min_churn mc
    -- Per Substack, necessary to set to earliest date in churned subscriptions
    where convert_timezone('US/Pacific', ss.subscription_created_at)::date >= mc.min_churn_dt
    group by
        start_dt,
        ss.user_id,
        ss.free_attribution

    union

    -- Include any users who have churned and not returned so
    -- wouldn't be in the subscription_stats table
    select

        convert_timezone('US/Pacific', cs.subscription_created_at)::date as start_dt,
        cs.user_id,
        cs.free_attribution,
        min(cs.last_seen_dt) as churn_dt

    from {{ ref('stg_substack__churned_subscriptions') }} cs
    cross join min_churn mc
    -- Per Substack, necessary to set to earliest date in churned subscriptions
    where convert_timezone('US/Pacific', cs.subscription_created_at)::date >= mc.min_churn_dt
    group by 
        start_dt,
        cs.user_id,
        cs.free_attribution

)

select * from final