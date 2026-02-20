-- models/marts/core/core/fct_subscription_renewals.sql

with

base as (

    select *
    from {{ ref('int_subscription_renewals') }}

),

historical as (

    select

        date_trunc('month', end_dt) as dt,
        *

    from base
    where date_trunc('month', end_dt) < date_trunc('month', current_date)

),

upcoming as (

    select

        date_trunc('month', end_dt) as dt,
        *

    from base
    where date_trunc('month', end_dt) > date_trunc('month', current_date)
    and refund_amount is null


),

current_month as (

    -- historical success payments
    select

        date_trunc('month', end_dt) as dt,
        *
    from base
    where end_dt <= current_date - 1
    and date_trunc('month', end_dt) >= date_trunc('month', current_date)
    and renewed = 1

    union all

    -- upcoming payments
    select

        date_trunc('month', end_dt) as dt,
        *
    from base
    where end_dt >= current_date
    and date_trunc('month', end_dt) >= date_trunc('month', current_date)
     and refund_amount is null

    union all

    -- unsuccessful payments
    select

        date_trunc('month', end_dt) as dt,
        *
    from base
    where end_dt <= current_date - 1
      and date_trunc('month', end_dt) >= date_trunc('month', current_date)
      and renewed = 0

),

joined as (

    select * from historical

    union all

    select * from upcoming

    union all

    select * from current_month

),

consolidated as (

    select distinct

        *

    from joined

),

final as (

    select

        -- ids
        expiring_user_invoice_id,
        stripe_subscription_id,
        renewal_user_invoice_id,
        user_id,

        -- attributes
        period_type,
        expiring_value,
        refund_amount,
        renewal_value,
        renewed,

        -- dates
        dt,
        end_dt,
        expiring_invoice_at,
        expiring_invoice_dt,
        renewed_at,
        renewed_date,
        end_month,
        is_historical_end_dt,
        is_upcoming_end_dt,

    from consolidated

)

select * from final