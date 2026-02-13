-- models/staging/substack/stg_user_invoices.sql

with

final as (

    select * from {{ source('substack', 'user_invoices') }}

)

select * from final