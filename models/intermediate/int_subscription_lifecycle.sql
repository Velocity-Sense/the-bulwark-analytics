-- models/intermediate/int_subscription_lifecycle.sql

with 

subscription_stats as (

    select * from {{ ref('stg_subscription_stats') }}

),

churned_subscriptions as (

    select * from {{ ref('stg_churned_subscriptions') }}

),

/*
Aggregate churned_subscriptions down to 1 row per subscription_id.
This prevents duplicate rows when joining to subscription_stats.
*/
churn_by_subscription as (

    select
        subscription_id,

        min(last_seen_dt)::date as churn_dt,
        min(unsubscribed_at)::timestamp as unsubscribed_at,
        max(subscription_expires_at)::timestamp as subscription_expires_at,

        /* lifecycle flags */
        max(iff(is_subscribed, 1, 0)) as is_subscribed,
        max(iff(is_paused, 1, 0)) as is_paused,
        max(iff(is_free_trial, 1, 0)) as is_free_trial,
        max(iff(is_lifetime, 1, 0)) as is_lifetime,
        max(iff(is_founding, 1, 0)) as is_founding,
        max(iff(is_gift, 1, 0)) as is_gift,
        max(iff(is_comp, 1, 0)) as is_comp,

        /* plan fields (choose max or any_value since they should be stable per subscription) */
        any_value(subscription_interval) as subscription_interval,
        any_value(stripe_plan_interval) as stripe_plan_interval,
        any_value(stripe_plan_interval_count) as stripe_plan_interval_count,
        any_value(stripe_plan_amount) as stripe_plan_amount,
        any_value(stripe_plan_currency) as stripe_plan_currency,
        any_value(stripe_plan_name) as stripe_plan_name,

        /* attribution + user metadata (these should be stable per subscription) */
        any_value(free_attribution) as free_attribution,
        any_value(paid_attribution) as paid_attribution,
        any_value(payment_source) as payment_source,

        /* engagement */
        any_value(activity_rating) as activity_rating,
        any_value(last_opened_at) as last_opened_at,
        any_value(last_seen_at) as last_seen_at,

        /* revenue */
        max(total_revenue_generated) as total_revenue_generated,
        max(total_revenue_refunded) as total_revenue_refunded,
        max(num_invoices_paid) as num_invoices_paid

    from churned_subscriptions
    where subscription_id is not null
    group by 1

),

/*
Promo signal.
Your established logic: earliest non-null event_data:coupon per user.
*/
promo_first_coupon as (

    select
        se.user_id,
        nullif(trim(se.event_data:coupon::string), '') as coupon_value
    from {{ ref('stg_subscription_events') }} se
    where se.user_id is not null
      and nullif(trim(se.event_data:coupon::string), '') is not null
    qualify row_number() over (
        partition by se.user_id
        order by se.timestamp
    ) = 1

),

final as (

    select
        ss.subscription_id,
        ss.user_id,

        /* canonical dates for cohort + lifecycle */
        convert_timezone('US/Pacific', ss.subscription_created_at)::date as start_dt,
        ss.subscription_created_at as start_ts,

        ss.first_payment_at,
        ss.last_payment_at,

        /* churn: prefer churn table signal when available */
        cbs.churn_dt,
        cbs.unsubscribed_at,
        cbs.subscription_expires_at,

        /* status flags */
        iff(ss.first_payment_at is not null, 1, 0) as is_paid,

        /* churned definition: if we have a churn_dt or unsubscribed_at, treat as churned */
        iff(cbs.churn_dt is not null or cbs.unsubscribed_at is not null, 1, 0) as is_churned,

        /* plan & access flags from churn snapshot when present */
        cbs.is_free_trial,
        cbs.is_lifetime,
        cbs.is_founding,
        cbs.is_gift,
        cbs.is_comp,
        cbs.is_paused,

        /* attribution: choose best available */
        coalesce(ss.paid_attribution, ss.free_attribution, cbs.paid_attribution, cbs.free_attribution, 'direct') as attribution_source,
        coalesce(ss.free_attribution, cbs.free_attribution) as free_attribution,
        coalesce(ss.paid_attribution, cbs.paid_attribution) as paid_attribution,
        cbs.payment_source,

        /*
          plan period: normalize to monthly vs annual.
          Override: lifetime subscriptions should be treated as annual-like.
        */
        case
            when coalesce(cbs.is_lifetime, 0) = 1 then 'annual'

            when lower(coalesce(
                ss.subscription_interval,
                ss.stripe_plan_interval,
                cbs.subscription_interval,
                cbs.stripe_plan_interval
            )) in ('year', 'annual', 'yearly') then 'annual'

            when coalesce(
                ss.stripe_plan_interval_count,
                cbs.stripe_plan_interval_count
            ) >= 12 then 'annual'

            else 'monthly'
        end as plan_period,

        /* promo vs full price */
        case
            when pfc.user_id is not null then 'promo'
            else 'full_price'
        end as price_bucket,

        pfc.coupon_value as promo_coupon_value,

        /* engagement fields (keep whichever source is populated) */
        coalesce(ss.activity_rating, cbs.activity_rating) as activity_rating,
        cbs.last_opened_at,
        cbs.last_seen_at,

        /* revenue fields (from churn snapshot when present) */
        cbs.total_revenue_generated,
        cbs.total_revenue_refunded,
        cbs.num_invoices_paid,

        /* convenience metric */
        datediff('day', convert_timezone('US/Pacific', ss.subscription_created_at)::date, cbs.churn_dt) as tenure_days

    from subscription_stats ss
    left join churn_by_subscription cbs
        on ss.subscription_id = cbs.subscription_id
    left join promo_first_coupon pfc
        on ss.user_id = pfc.user_id

)

select *
from final