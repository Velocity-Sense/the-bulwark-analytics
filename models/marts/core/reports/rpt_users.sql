-- models/marts/core/reports/rpt_users.sql

with final as (

    select * from {{ ref('int_users') }}

)

select * from final