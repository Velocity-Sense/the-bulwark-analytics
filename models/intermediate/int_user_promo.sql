-- models/intermediate/int_user_promo.sql

with

subscription_events as (

    select * from {{ ref('stg_substack__subscription_events') }}

),

final as (

    select

        user_id,
        nullif(trim(event_data:coupon::string), '') as coupon_value,
        min(timestamp)::timestamp as first_coupon_ts

    from subscription_events
    where user_id is not null
    and nullif(trim(event_data:coupon::string), '') is not null
    and event = 'User Subscribed'
    group by 1, 2
    qualify row_number() over (
        partition by user_id
        order by first_coupon_ts
    ) = 1

)

select * from final