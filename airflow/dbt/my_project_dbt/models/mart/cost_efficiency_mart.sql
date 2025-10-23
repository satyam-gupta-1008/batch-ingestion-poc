{{
    config(
        materialized='table'
    )
}}

select
    geo,
    date::date as activity_date,
    sum(nullif(cost, '')::numeric) as total_cost,
    sum(nullif(users, '')::numeric) as total_users,
    round(avg(nullif(ecpi, '')::numeric), 2) as avg_ecpi,
    case when sum(nullif(users, '')::numeric) > 0 
         then round(sum(nullif(cost, '')::numeric) / sum(nullif(users, '')::numeric), 2) 
         else null end as actual_cpi
from {{ ref('airbyte') }}
group by geo, date::date
order by geo, activity_date
