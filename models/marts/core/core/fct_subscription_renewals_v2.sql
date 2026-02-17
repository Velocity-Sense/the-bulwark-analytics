-- models/intermediate/int_subscription_renewals_cohorts.sql

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

future as (

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

    select * from future

    union all

    select * from current_month

),

final as (

    select distinct

        *

        -- dt,
        -- user_id,
        -- period_type

    from joined

)

select * from final