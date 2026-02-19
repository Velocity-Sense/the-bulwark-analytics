-- models/staging/substack/stg_substack__churned_subscriptions.sql

with

base as (

    select * from {{ source('substack', 'churned_subscriptions') }}

),

final as (

    select

        -- ids
        user_id,
        subscription_id,
        publication_id,
        group_parent_subscription_id,

        -- attributes
        user_email_address,
        user_name,
        activity_rating,
        country,
        state,
        city,
        time_zone,
        subscription_interval,
        subscription_type,
        payment_source,
        membership_state,
        bestseller_tier,
        free_attribution,
        paid_attribution,
        sections_enabled,

        -- stripe
        stripe_plan_amount,
        stripe_plan_currency,
        stripe_plan_interval,
        stripe_plan_interval_count,
        stripe_plan_quantity,
        stripe_plan_name,

        -- quant
        num_invoices_paid,
        total_revenue_refunded,
        total_revenue_generated,

        -- booleans
        is_subscribed,
        is_paused,
        is_founding,
        is_paying_regular_member,
        is_lifetime,
        is_comp,
        is_bitcoin,
        is_gift,
        is_free_trial,
        is_group_parent,
        is_group_member,
        root_enabled,
        user_has_publication,

        -- date
        unsubscribed_at,
        last_subscribed_at,
        subscription_created_at,
        subscription_expires_at,
        subscription_updated_at,
        last_seen_dt,
        last_seen_at,
        last_opened_at,
        email_disabled_at,
        first_payment_at,
        last_payment_at,

        -- data freshness
        data_updated_at as _data_updated_at

    from base

)

select * from final