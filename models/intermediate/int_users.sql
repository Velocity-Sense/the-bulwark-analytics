-- models/intermediate/int_users.sql

with final as (

    select * from {{ ref('stg_substack__subscription_stats') }}

)

select
    user_id,
    user_name,
    user_email_address,

    /* geo */
    upper(nullif(trim(country), '')) as country,
    upper(nullif(trim(state), '')) as state,
    nullif(trim(city), '') as city,
    upper(nullif(trim(subscription_country), '')) as subscription_country,
    time_zone,
    locale,

    /* attribution */
    coalesce(nullif(trim(paid_attribution), ''), nullif(trim(free_attribution), ''), 'direct') as attribution_source,
    nullif(trim(free_attribution), '') as free_attribution,
    nullif(trim(paid_attribution), '') as paid_attribution,
    nullif(trim(payment_source), '') as payment_source,

    /* membership flags */
    is_subscribed,
    is_paying_regular_member,
    is_founding,
    is_lifetime,
    is_gift,
    is_comp,
    is_free_trial,
    is_paused,
    membership_state,

    /* engagement */
    activity_rating,
    num_emails_received,
    num_emails_opened,
    links_clicked,
    num_web_post_views,
    num_unique_web_posts_seen,
    num_comments,
    num_shares,

    /* recency fields */
    last_opened_at,
    last_clicked_at,
    last_subscribed_at,
    days_active_last_30d,

    /* commercial */
    total_revenue_generated,
    total_revenue_refunded,
    num_invoices_paid,

    /* lifecycle timestamps */
    subscription_created_at,
    subscription_updated_at,
    first_payment_at,
    last_payment_at,
    unsubscribed_at,
    subscription_expires_at,

    /* metadata */
    data_updated_at

from final