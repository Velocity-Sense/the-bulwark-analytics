-- models/intermediate/operations/int_march_2026_annual_renewal_cohort.sql

select *
from {{ ref('int_upcoming_annual_renewal_cohort') }}
where end_month = '2026-03-01'