-- models/staging/substack/stg_substack__subscription_events.sql

with

base as (

    select * from {{ source('substack', 'subscription_events') }}

),

final as (

    select

        id as event_id,
        * exclude (id)
    
    from base

)

select * from final