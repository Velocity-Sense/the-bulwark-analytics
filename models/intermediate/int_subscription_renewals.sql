-- models/intermediate/int_subscription_renewals.sql
with

user_invoices as (

    select * from {{ ref('stg_substack__user_invoices') }}

),

invoices as (

    select
        user_invoice_id,
        user_id,
        stripe_subscription_id,

        timestamp::timestamp as invoice_ts,
        timestamp::date as invoice_dt,

        adjusted_end_timestamp::timestamp as adjusted_end_ts,
        adjusted_end_timestamp::date as end_dt,

        period,
        refund_amount,
        amount / 100.0 as amount_usd

    from user_invoices
    where stripe_subscription_id is not null
      and adjusted_end_timestamp is not null
      and timestamp is not null
),

renewal_matches as (

    select
        og.user_id,
        og.stripe_subscription_id,
        og.user_invoice_id as expiring_user_invoice_id,

        og.invoice_ts as expiring_invoice_at,
        og.invoice_dt as expiring_invoice_dt,
        og.end_dt,
        date_trunc('month', og.end_dt) as end_month,

        case when og.period > 30000000 then 'annual' else 'monthly' end as period_type,

        og.amount_usd as expiring_value,
        og.refund_amount,

        rn.user_invoice_id as renewal_user_invoice_id,
        rn.amount_usd as renewal_value,
        rn.invoice_ts as renewed_at,
        rn.invoice_dt as renewed_date,

        iff(rn.user_invoice_id is not null, 1, 0) as renewed,

        iff(og.end_dt < current_date, 1, 0) as is_historical_end_dt,
        iff(og.end_dt >= current_date, 1, 0) as is_upcoming_end_dt

    from invoices og
    left join invoices rn
        on og.user_id = rn.user_id
        and og.adjusted_end_ts::date = rn.invoice_ts::date
    where og.stripe_subscription_id is not null
    qualify row_number() over (
        partition by og.user_invoice_id
        order by rn.invoice_ts
    ) = 1
)

select *
from renewal_matches