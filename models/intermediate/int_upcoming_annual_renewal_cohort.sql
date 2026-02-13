-- models/intermediate/int_upcoming_annual_renewal_cohort.sql

with renewal_opp as (

    select
        user_id,
        stripe_subscription_id,
        expiring_user_invoice_id,

        end_dt,
        date_trunc('month', end_dt) as end_month,

        period_type,
        refund_amount,

        renewed,
        expiring_value,
        renewal_value,

        renewal_number_user,
        renewal_number_subscription

    from {{ ref('int_invoice_renewal_opportunity') }}
    where period_type = 'annual'
      and refund_amount is null
      and end_dt is not null
      and end_dt >= current_date
      and end_dt < current_date + interval '12 months'

),

lifecycle as (

    select
        subscription_id,
        user_id,

        start_dt,
        date_trunc('month', start_dt) as start_cohort_month,

        plan_period,
        price_bucket,
        promo_coupon_value,

        attribution_source,
        free_attribution,
        paid_attribution,

        is_paid,
        is_founding,
        is_lifetime,
        is_gift,
        is_comp,

        activity_rating,
        tenure_days,

        churn_dt,
        is_churned

    from {{ ref('int_subscription_lifecycle') }}

),

joined as (

    select
        ro.user_id,
        ro.stripe_subscription_id,
        ro.expiring_user_invoice_id,

        ro.end_dt,
        ro.end_month,

        ro.period_type,
        ro.renewed,

        ro.expiring_value,
        ro.renewal_value,

        ro.renewal_number_user,
        ro.renewal_number_subscription,

        /* lifecycle enrichment */
        l.subscription_id,
        l.start_dt,
        l.start_cohort_month,
        l.plan_period,
        l.price_bucket,
        l.promo_coupon_value,
        l.attribution_source,
        l.free_attribution,
        l.paid_attribution,
        l.is_paid,
        l.is_founding,
        l.is_lifetime,
        l.is_gift,
        l.is_comp,
        l.activity_rating,
        l.tenure_days,
        l.churn_dt,
        l.is_churned

    from renewal_opp ro
    left join lifecycle l
        on ro.user_id = l.user_id

)

select *
from joined