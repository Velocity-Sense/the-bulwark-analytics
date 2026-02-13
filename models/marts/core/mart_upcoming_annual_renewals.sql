-- models/marts/operations/mart_upcoming_annual_renewals.sql

with final as (

    select * from {{ ref('int_upcoming_annual_renewal_cohort') }}

)

select * from final