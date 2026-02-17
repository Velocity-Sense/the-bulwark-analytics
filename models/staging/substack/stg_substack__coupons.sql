-- models/staging/substack/stg_substack__coupons.sql

with 

base as (

    select * from {{ source('substack', 'coupons') }}

),

final as (

    select

        id as coupon_id,
        * exclude (id)

    from base

)

select * from final