-- models/intermediate/int_upcoming_annual_renewals_engagement.sql

with labeled_users as (

    select
        user_id,
        case
            when end_month = '2026-03-01' then 'March 2026'
            else 'All other cohorts'
        end as cohort_label
    from {{ ref('int_upcoming_annual_renewal_cohort') }}

),

final as (

    select
        lu.cohort_label as cohort,
        count(distinct lu.user_id) as users,

        sum(ss.num_emails_received) as num_emails_received,
        sum(ss.num_emails_opened) as num_emails_opened,
        sum(ss.links_clicked) as links_clicked,
        avg(ss.activity_rating) as avg_activity_rating,

        sum(ss.num_emails_opened) / nullif(sum(ss.num_emails_received), 0) as open_rate,
        sum(ss.links_clicked) / nullif(sum(ss.num_emails_opened), 0) as click_to_open_rate

    from labeled_users lu
    join {{ ref('stg_subscription_stats') }} ss
        on ss.user_id = lu.user_id
    group by 1
)

select *
from final
order by cohort