{{
    config(
        materialized='table'
    )
}}

select
    geo,
    date::date as activity_date,
    sum(af_purchase_sum_day_1::numeric) as purchase_sum_day_1,
    sum(af_purchase_sum_day_7::numeric) as purchase_sum_day_7,
    sum(af_purchase_sum_day_30::numeric) as purchase_sum_day_30,
    sum(af_purchase_sum_day_90::numeric) as purchase_sum_day_90,
    sum(af_purchase_count_day_1::numeric) as purchase_count_day_1,
    sum(af_purchase_count_day_7::numeric) as purchase_count_day_7,
    sum(af_purchase_count_day_30::numeric) as purchase_count_day_30,
    sum(af_purchase_count_day_90::numeric) as purchase_count_day_90
from {{ ref('airbyte') }}
group by geo, date::date
order by geo, activity_date
