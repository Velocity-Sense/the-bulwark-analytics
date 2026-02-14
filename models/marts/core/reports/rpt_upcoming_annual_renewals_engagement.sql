-- models/marts/core/reports/rpt_upcoming_annual_renewals_engagement.sql

with final as (

    select * from {{ ref('int_upcoming_annual_renewals_engagement') }}

)

select * from final