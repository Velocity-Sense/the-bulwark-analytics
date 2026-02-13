-- models/intermediate/int_invoice_renewal_opportunity.sql

with lifecycle as (

    /*
      Lifetime plans often do not have a normal invoice period length.
      Treat any user with a lifetime subscription as annual-like for segmentation.

      NOTE: This is user_id-based. If you later add a stronger bridge between
      stripe_subscription_id and subscription_id, update this to be subscription-specific.
    */
    select
        user_id,
        max(iff(is_lifetime = 1, 1, 0)) as is_lifetime_user
    from {{ ref('int_subscription_lifecycle') }}
    group by 1

),

invoices as (

    select
        ui.user_invoice_id,
        ui.user_id,
        ui.stripe_subscription_id,

        ui.timestamp::date as invoice_dt,
        ui.adjusted_end_timestamp::date as end_dt,

        ui.amount / 100.0 as invoice_amount,
        ui.refund_amount,
        ui.period,

        /*
          Base classification from invoice period.
          Override: lifetime users should be treated as annual-like.
        */
        case
            when coalesce(lc.is_lifetime_user, 0) = 1 then 'annual'
            when ui.period > 30000000 then 'annual'
            else 'monthly'
        end as period_type

    from {{ ref('stg_user_invoices') }} ui
    left join lifecycle lc
        on ui.user_id = lc.user_id
    where ui.stripe_subscription_id is not null
      and ui.adjusted_end_timestamp is not null
      and ui.timestamp is not null

),

renewal_labeled as (

    /*
      Renewal decision at each term end:
      renewed = 1 if another invoice exists on the end_dt (invoice_dt of the next invoice)
    */
    select
        og.user_id,
        og.stripe_subscription_id,
        og.user_invoice_id as expiring_user_invoice_id,

        og.invoice_dt,
        og.end_dt,
        og.period,
        og.period_type,

        og.invoice_amount as expiring_value,

        iff(rn.user_invoice_id is not null, 1, 0) as renewed,
        rn.invoice_amount as renewal_value,

        og.refund_amount

    from invoices og
    left join invoices rn
        on og.user_id = rn.user_id
        and og.end_dt = rn.invoice_dt

),

numbered as (

    select
        *,
        iff(end_dt < current_date, 1, 0) as is_historical_end_dt,

        row_number() over (
            partition by user_id
            order by end_dt
        ) as renewal_number_user,

        row_number() over (
            partition by stripe_subscription_id
            order by end_dt
        ) as renewal_number_subscription

    from renewal_labeled
    -- IMPORTANT: do NOT filter to historical here.
    -- This model needs to support upcoming renewals (ex: March 2026).
)

select *
from numbered