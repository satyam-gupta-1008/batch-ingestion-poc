{{
    config(
        materialized='table'
    )
}}

select
    geo,
    date::date as activity_date,
    round(avg(af_purchase_conversion_rate_day_1::numeric), 4) as conv_rate_day_1,
    round(avg(af_purchase_conversion_rate_day_7::numeric), 4) as conv_rate_day_7,
    round(avg(af_purchase_conversion_rate_day_30::numeric), 4) as conv_rate_day_30,
    round(avg(af_purchase_conversion_rate_day_90::numeric), 4) as conv_rate_day_90
from {{ ref('airbyte') }}
group by geo, date::date
order by geo, activity_date
