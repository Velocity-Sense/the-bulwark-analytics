-- models/marts/core/core/fct_user_coupons.sql
-- grain: 1 row per user per coupon used

with

user_coupons as (

    select * from {{ ref('int_user_coupons') }}

),

coupons as (

    select * from {{ ref('stg_substack__coupons') }}

),

final as (

    select

        -- ids
        uc.user_id,
        uc.coupon_id,

        -- coupon attributes
        c.name              as coupon_name,
        c.code              as coupon_code,
        c.type              as coupon_type,
        c.percent_off,
        c.duration,
        c.duration_in_months,
        c.trial_period_days,

        -- flags
        case when c.trial_period_days is not null then true else false end as is_trial_promo,
        c.valid             as is_valid,

        -- metrics
        uc.times_coupon_applied,
        uc.first_coupon_ts

    from user_coupons uc
    left join coupons c
        on uc.coupon_id = c.coupon_id

)

select * from final
