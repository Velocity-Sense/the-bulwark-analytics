-- models/marts/core/core/dim_subscription_renewal_users.sql
-- grain: 1 record per user based on latest renewal

with 

base as (

    select * from {{ ref('fct_subscription_renewals') }}

),

latest_renewal as (

    select
        *
    from base
    qualify row_number() over (
        partition by user_id
        order by end_dt desc
    ) = 1

),

int_users as (
    
    select * from {{ ref('int_users') }}
    
),

final as (

    select 
    
        lr.user_id,
        u.* exclude (user_id)
        
    from latest_renewal lr
    left join int_users u
        on lr.user_id = u.user_id

)

select * from final