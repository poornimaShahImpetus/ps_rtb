with payment_summary as (
    select
        p.order_id,
        sum(p.amount) as total_payment_amount
    from {{ ref('stg_payments') }} p
    group by p.order_id
),

order_details as (
    select
        o.order_id,
        o.customer_id,
        o.order_date,
        o.status
    from {{ ref('stg_orders') }} o
)

select
    o.order_id,
    o.customer_id,
    o.order_date,
    o.status,
    coalesce(ps.total_payment_amount, 0) as total_payment_amount
from order_details o
left join payment_summary ps
    on o.order_id = ps.order_id