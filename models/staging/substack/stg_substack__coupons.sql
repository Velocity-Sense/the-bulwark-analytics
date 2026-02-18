-- models/staging/substack/stg_substack__coupons.sql

with 

base as (

    select * from {{ source('substack', 'coupons') }}

),

final as (

    select

        -- ids
        id as coupon_id,
        stripe_account_id,
        stripe_coupon_id,
        publication_id,

        -- attributes
        name,
        description,
        type,
        code,

        -- booleans
        valid,
        edu_only,
        yearly_only,
        monthly_only,
        group_only,
        allowed_suffixes,
        hidden,
        founding_only,

        -- quant
        percent_off,
        max_redemptions,
        duration,
        duration_in_months,
        trial_period_days,
        extra_seats,
        
        -- date
        created_at,
        redeem_by,

        -- data freshness
        data_updated_at as _data_updated_at

    from base

)

select * from final