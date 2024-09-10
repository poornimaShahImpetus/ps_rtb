with payment_method_summary as (
    select
        p.order_id,
        count(distinct p.payment_method) as payment_method_count
    from {{ ref('stg_payments') }} p
    group by p.order_id
)

select
    o.order_id,
    o.customer_id,
    o.order_date,
    o.status,
    pms.payment_method_count
from {{ ref('stg_orders') }} o
join payment_method_summary pms
    on o.order_id = pms.order_id
where pms.payment_method_count > 1