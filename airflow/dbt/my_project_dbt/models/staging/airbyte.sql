with source as (
    select * 
    from "{{ var('source_name') }}"."{{ var('table_name') }}"
),
de_du as (
    select
        *,
        row_number() over (partition by date order by geo desc) as rn
    from source
)

select
    c,
    geo,
    pid,
    cost,
    date,
    ecpi,
    users,
    af_c_id,
    af_purchase_sum_day_1,
    af_purchase_sum_day_7,
    af_purchase_sum_day_30,
    af_purchase_sum_day_90,
    af_purchase_count_day_1,
    af_purchase_count_day_7,
    af_purchase_count_day_30,
    af_purchase_count_day_90,
    af_purchase_unique_users_day_1,
    af_purchase_unique_users_day_7,
    af_purchase_unique_users_day_30,
    af_purchase_unique_users_day_90,
    af_purchase_conversion_rate_day_1,
    af_purchase_conversion_rate_day_7,
    af_purchase_conversion_rate_day_30,
    af_purchase_conversion_rate_day_90,
    rn
from de_du
where rn = 1
