-- models/marts/core/reports/rpt_coupons_performance.sql
-- grain: 1 row per user who used coupon 67b7bcc2 - BLEAK FRIDAY SALE: GET A YEAR OF THE BULWARK FOR JUST $75
-- classifies each user's renewal outcome after their annual promo expired
-- If reusing for other promos, ensure to remove user_id cleanup logic in promo_annual_row CTE

with

promo_users as (

    select distinct
        user_id,
        first_coupon_ts
    from {{ ref('fct_user_coupons') }}
    where coupon_id = '67b7bcc2'

),

renewals as (

    select
        renewals.user_id,
        renewals.stripe_subscription_id,
        renewals.expiring_user_invoice_id,
        renewals.expiring_invoice_dt,
        renewals.expiring_value,
        renewals.end_dt,
        renewals.period_type,
        renewals.renewed,
        renewals.renewal_user_invoice_id,
        renewals.renewal_value,
        renewals.refund_amount,
        promo_users.first_coupon_ts
    from {{ ref('fct_subscription_renewals') }} as renewals
    inner join promo_users
        on renewals.user_id = promo_users.user_id

),

-- the original annual promo row: period_type = 'annual' and expiring_value = 75
promo_annual_row as (

    select
        user_id,
        stripe_subscription_id,
        expiring_user_invoice_id,
        expiring_invoice_dt,
        expiring_value,
        end_dt,
        renewed,
        renewal_user_invoice_id,
        renewal_value,
        refund_amount,
        first_coupon_ts
    from renewals
    where period_type = 'annual'
    and (
            expiring_value = 75
            -- data correction: these users have incorrect expiring_value
            or user_id in (203325343, 21716585, 476762)
        )
    qualify row_number() over (
        partition by user_id
        order by end_dt asc
    ) = 1

),

-- check if the user has any monthly rows after their promo annual end_dt
has_monthly_after_promo as (

    select
        renewals.user_id,
        min(renewals.expiring_invoice_dt) as first_monthly_invoice_dt,
        max(renewals.expiring_value) as monthly_expiring_value
    from renewals
    inner join promo_annual_row
        on renewals.user_id = promo_annual_row.user_id
    where renewals.period_type = 'monthly'
      and renewals.expiring_invoice_dt >= promo_annual_row.end_dt - interval '30 days'
    group by 1

),

classified as (

    select
        promo_annual_row.user_id,
        promo_annual_row.stripe_subscription_id,
        promo_annual_row.expiring_user_invoice_id,
        promo_annual_row.expiring_invoice_dt,
        promo_annual_row.expiring_value as promo_expiring_value,
        promo_annual_row.end_dt as promo_end_dt,
        promo_annual_row.renewed as promo_annual_renewed,
        promo_annual_row.renewal_user_invoice_id,
        promo_annual_row.renewal_value,
        promo_annual_row.refund_amount,
        promo_annual_row.first_coupon_ts,

        case
            when promo_annual_row.refund_amount is not null
                then 'refunded'
            when promo_annual_row.renewed = 1
                then 'renewed_annual'
            when monthly.user_id is not null
                then 'renewed_monthly'
            when promo_annual_row.end_dt >= current_date
                then 'pending'
            else 'churned'
        end as renewal_outcome,

        monthly.first_monthly_invoice_dt,
        monthly.monthly_expiring_value

    from promo_annual_row
    left join has_monthly_after_promo as monthly
        on promo_annual_row.user_id = monthly.user_id

),

final as (

    select

        -- ids
        user_id,
        stripe_subscription_id,
        expiring_user_invoice_id,
        renewal_user_invoice_id,

        -- promo details
        promo_expiring_value,
        promo_end_dt,
        expiring_invoice_dt,
        first_coupon_ts,

        -- outcome
        renewal_outcome,
        promo_annual_renewed,
        renewal_value,
        refund_amount,

        -- monthly switch details
        first_monthly_invoice_dt,
        monthly_expiring_value

    from classified

)

select * from final
