-- models/marts/core/core/dim_subscription_renewal_users.sql

with 

base as (

    select * from {{ ref('fct_subscription_renewals') }}

),

int_users as (
    
    select * from {{ ref('int_users') }}
    
),

final as (

    select 
    
        b.user_id,
        u.* exclude (user_id)
        
    from base b
    left join int_users u
        on b.user_id = u.user_id

)

select * from final