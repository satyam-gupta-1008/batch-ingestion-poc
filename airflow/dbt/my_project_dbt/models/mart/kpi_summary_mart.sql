{{
    config(
        materialized='table'
    )
}}

with revenue as (
    select * from {{ ref('revenue_summary_mart') }}
),
conversion as (
    select * from {{ ref('user_conversion_mart') }}
),
costs as (
    select * from {{ ref('cost_efficiency_mart') }}
)

select
    r.geo,
    r.activity_date,

    -- revenue
    r.purchase_sum_day_1,
    r.purchase_sum_day_7,
    r.purchase_sum_day_30,
    r.purchase_sum_day_90,

    -- conversion
    c.conv_rate_day_1,
    c.conv_rate_day_7,
    c.conv_rate_day_30,
    c.conv_rate_day_90,

    -- costs
    co.total_cost,
    co.total_users,
    co.avg_ecpi,
    co.actual_cpi,

    -- derived KPI: revenue per user (LTV)
    case when co.total_users > 0 
         then round(r.purchase_sum_day_30::numeric / co.total_users, 2)
         else null end as ltv_day_30,

    -- derived KPI: ROI (30-day revenue / total cost)
    case when co.total_cost > 0 
         then round(r.purchase_sum_day_30::numeric / co.total_cost, 2)
         else null end as roi_day_30

from revenue r
left join conversion c 
    on r.geo = c.geo and r.activity_date = c.activity_date
left join costs co
    on r.geo = co.geo and r.activity_date = co.activity_date
order by r.geo, r.activity_date
