-- models/intermediate/int_user_coupons.sql
-- grain: 1 row per user per coupon used

with

subscription_events as (

    select * from {{ ref('stg_substack__subscription_events') }}

),

final as (

    select

        user_id,
        nullif(trim(event_data:coupon::string), '') as coupon_id,
        min(timestamp)::timestamp as first_coupon_ts,
        count(*) as times_coupon_applied

    from subscription_events
    where user_id is not null
      and nullif(trim(event_data:coupon::string), '') is not null
      and event = 'User Subscribed'
    group by
        user_id,
        coupon_id

)

select * from final
