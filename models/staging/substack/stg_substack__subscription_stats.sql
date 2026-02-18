-- models/staging/substack/stg_substack__subscription_stats.sql

with

base as (

    select * from {{ source('substack', 'subscription_stats') }}

),

final as (

    select

        -- ids
        subscription_id,
        user_id,
        publication_id,
        group_parent_subscription_id,

        -- attributes
        user_name,
        user_email_address,
        sections_enabled,
        subscription_interval,
        subscription_type,
        country,
        state,
        city,
        locale,
        time_zone,
        payment_source,
        stripe_plan_amount,
        stripe_plan_currency,
        stripe_plan_interval,
        stripe_plan_interval_count,
        stripe_plan_quantity,
        stripe_plan_name,
        membership_state,
        subscription_country,
        user_has_publication,
        bestseller_tier,
        free_attribution,
        paid_attribution,

        activity_rating as activity_rating_src,  -- Substack issue rating 1-6. Will normalize 0-5.
        -- Temp fix until Substack resolves rating 1-6
        -- Expecting 0-5 rating
        case
            when activity_rating is not null
            then activity_rating - 1
        end as activity_rating,

        -- booleans
        is_subscribed,
        is_founding,
        is_paying_regular_member,
        is_lifetime,
        is_comp,
        is_bitcoin,
        is_gift,
        is_free_trial,
        is_group_parent,
        is_group_member,
        is_paused,
        root_enabled,

        -- quant
        links_clicked,
        num_emails_received,
        num_emails_received_last_7d,
        num_emails_received_last_30d,
        num_emails_dropped,
        num_emails_dropped_last_7d,
        num_emails_dropped_last_30d,
        num_emails_opened,
        num_email_opens,
        num_email_opens_last_7d,
        num_email_opens_last_30d,
        num_unique_email_posts_seen,
        num_unique_email_posts_seen_last_7d,
        num_unique_email_posts_seen_last_30d,
        num_web_post_views,
        num_web_post_views_last_7d,
        num_web_post_views_last_30d,
        num_unique_web_posts_seen,
        num_unique_web_posts_seen_last_7d,
        num_unique_web_posts_seen_last_30d,
        num_comments,
        num_comments_last_7d,
        num_comments_last_30d,
        num_shares,
        num_shares_last_7d,
        num_shares_last_30d,
        num_subs_gifted,
        num_invoices_paid,
        total_revenue_refunded,
        total_revenue_generated,
        days_active_last_30d,

        -- dates
        last_subscribed_at,
        last_opened_at,
        email_disabled_at,
        first_payment_at,
        last_payment_at,
        last_clicked_at,
        unsubscribed_at,
        subscription_created_at,
        subscription_expires_at,
        subscription_updated_at,
        
        -- data freshness
        data_updated_at as _data_updated_at

    from base

)

select * from final