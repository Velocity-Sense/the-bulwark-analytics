-- models/marts/core/reports/rpt_subscription_renewals.sql

with 

base as (

    select *
    from {{ ref('fct_subscription_renewals_v2') }}

),

historical as (

    select

        date_trunc('month', end_dt) as dt,
        period_type,

        /* upcoming pipeline metric is null for historical rows */
        null::number as upcoming_payments,

        /* NEW: monetary metrics */
        sum(iff(renewed = 1, renewal_value, null)) as paid_amount,
        null::float as upcoming_expected_amount,

        count(distinct user_id) as opportunity_value,
        count(distinct iff(renewed = 1, user_id, null)) as successful_payments,
        count(distinct user_id)
          - count(distinct iff(renewed = 1, user_id, null)) as unsuccessful_payments,
        null::float as renewal_rate

    from base
    where end_dt <= current_date - 1
      and date_trunc('month', end_dt) >= date_trunc('month', current_date) - interval '6 months'
    group by 1, 2

),

upcoming as (

    select

        date_trunc('month', end_dt) as dt,
        period_type,

        /* upcoming payments: future opportunities in next 12 months (not refunded) */
        count(distinct user_id) as upcoming_payments,

        /* NEW: monetary metrics */
        null::float as paid_amount,
        sum(expiring_value) as upcoming_expected_amount,

        /* historical performance metrics are null for upcoming rows */
        null::number as successful_payments,
        null::number as opportunity_value,
        null::number as unsuccessful_payments,
        null::float as renewal_rate

    from base
    where end_dt >= current_date
      and end_dt < current_date + interval '12 months'
      and refund_amount is null
    group by 1, 2

),

unioned as (

    select

        dt,
        period_type,
        upcoming_payments,
        paid_amount,
        upcoming_expected_amount,
        successful_payments,
        opportunity_value,
        unsuccessful_payments

    from historical

    union all

    select

        dt,
        period_type,
        upcoming_payments,
        paid_amount,
        upcoming_expected_amount,
        successful_payments,
        opportunity_value,
        unsuccessful_payments

    from upcoming

)

select

    dt,
    period_type,

    /* counts */
    sum(upcoming_payments) as upcoming_payments,
    sum(successful_payments) as successful_payments,
    sum(opportunity_value) as opportunity_value,
    sum(unsuccessful_payments) as unsuccessful_payments,

    /* monetary */
    sum(paid_amount) as paid_amount,
    sum(upcoming_expected_amount) as upcoming_expected_amount,

    /* renewal rate: current month includes upcoming pipeline in denominator */
    case
        when dt = date_trunc('month', current_date) then
            sum(successful_payments)
              / nullif(sum(opportunity_value) + sum(upcoming_payments), 0)
        else
            sum(successful_payments)
              / nullif(sum(opportunity_value), 0)
    end as renewal_rate

from unioned
group by 1, 2
order by dt, period_type