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

    /* engagement (lifetime / raw) */
    activity_rating,
    num_emails_received,
    num_emails_opened as num_emails_opened_lifetime,
    num_email_opens as num_email_opens_lifetime,
    links_clicked,
    num_web_post_views,
    num_unique_web_posts_seen,
    num_comments,
    num_shares,

    /* engagement (last 30d) */
    last_opened_at,
    last_clicked_at,
    last_subscribed_at,
    days_active_last_30d,
    num_emails_received_last_7d,
    num_emails_received_last_30d,
    num_email_opens_last_7d,
    num_email_opens_last_30d,
    num_unique_email_posts_seen_last_7d,
    num_unique_email_posts_seen_last_30d,
    num_emails_dropped_last_7d,
    num_emails_dropped_last_30d,
    num_web_post_views_last_7d,
    num_web_post_views_last_30d,
    num_unique_web_posts_seen_last_7d,
    num_unique_web_posts_seen_last_30d,
    num_comments_last_7d,
    num_comments_last_30d,
    num_shares_last_7d,
    num_shares_last_30d,

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
    _data_updated_at,

    /* =========================
       simplified churn-focused engagement metrics
       ========================= */

    /* email open rate (30d) */
    case
        when coalesce(num_emails_received_last_30d, 0) > 0
        then round((num_email_opens_last_30d::float / nullif(num_emails_received_last_30d, 0)) * 100, 2)
        else 0
    end as email_open_rate_30d_pct,

    /* web breadth (30d) */
    coalesce(num_unique_web_posts_seen_last_30d, 0) as web_posts_seen_30d,

    /* community actions (30d) */
    (coalesce(num_comments_last_30d, 0) + coalesce(num_shares_last_30d, 0)) as community_actions_30d,

    /* days since last open (recency) */
    case
        when last_opened_at is not null
        then datediff('day', last_opened_at, current_date())
        else null
    end as days_since_open,

    /* engagement score (0–100) weights:
       - recency: 40%
       - active days: 30%
       - email open rate: 20%
       - web breadth: 10%
    */

    ifnull(round(
        (
            /* recency score: 1 if opened in last 7d, 0 if >60d */
            (case
                when last_opened_at is null then 0
                when datediff('day', last_opened_at, current_date()) <= 7 then 1
                when datediff('day', last_opened_at, current_date()) >= 60 then 0
                else 1 - (datediff('day', last_opened_at, current_date()) - 7)::float / 53
            end) * 0.40

            + (least(greatest(coalesce(days_active_last_30d, 0)::float / 30, 0), 1) * 0.30)

            + (
                least(
                    greatest(
                        (coalesce(num_email_opens_last_30d, 0)::float
                         / nullif(coalesce(num_emails_received_last_30d, 0), 0)
                        ),
                    0),
                1) * 0.20
            )

            + (least(coalesce(num_unique_web_posts_seen_last_30d, 0)::float / 8, 1) * 0.10)
        ) * 100
    , 2), 0) as engagement_score

from final