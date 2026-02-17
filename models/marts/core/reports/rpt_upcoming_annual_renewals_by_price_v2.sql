-- models/marts/core/reports/rpt_upcoming_annual_renewals_by_price.sql
-- Excludes free-trial promos

with 

subscription_renewals as (

    select * from {{ ref('fct_subscription_renewals_v2') }}

),

user_promo as (

    select * from {{ ref('int_user_promo') }}

),

upcoming as (

  select

    date_trunc('month', end_dt) as end_month,
    user_id,
    expiring_value

  from subscription_renewals
  where period_type = 'annual'
    and end_dt >= current_date
    and end_dt < current_date + interval '12 months'
    and refund_amount is null
),

labeled as (

  select
    u.end_month,
    case when p.user_id is not null and p.is_trial_promo = false then 'promo'
        else 'full_price' end as price_bucket,
    u.user_id,
    u.expiring_value

  from upcoming u
  left join user_promo p
    on u.user_id = p.user_id
),

final as (

select

  end_month,
  price_bucket,
  count(distinct user_id) as upcoming_users,
  sum(expiring_value) as upcoming_expected_amount

from labeled
group by 1, 2
order by 
    end_month, 
    price_bucket

)

select * from final