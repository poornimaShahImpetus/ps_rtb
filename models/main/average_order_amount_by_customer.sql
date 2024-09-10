with order_totals as (
    select
        o.customer_id,
        count(o.order_id) as order_count,
        sum(p.amount) as total_amount
    from {{ ref('stg_orders') }} o
    join {{ ref('stg_payments') }} p
        on o.order_id = p.order_id
    group by o.customer_id
),

average_order_amount as (
    select
        customer_id,
        total_amount / nullif(order_count, 0) as average_order_amount
    from order_totals
)

select
    c.customer_id,
    c.first_name,
    c.last_name,
    a.average_order_amount
from {{ ref('stg_customers') }} c
left join average_order_amount a
    on c.customer_id = a.customer_id
