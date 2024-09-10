with customer_info as (
    select
        c.customer_id,
        c.first_name,
        c.last_name
    from {{ ref('stg_customers') }} c
),

order_details as (
    select
        o.customer_id,
        min(o.order_date) as first_order_date,
        max(o.order_date) as most_recent_order_date,
        count(o.order_id) as total_orders
    from {{ ref('stg_orders') }} o
    group by o.customer_id
),

payment_details as (
    select
        o.customer_id,
        sum(p.amount) as lifetime_value
    from {{ ref('stg_orders') }} o
    join {{ ref('stg_payments') }} p
        on o.order_id = p.order_id
    group by o.customer_id
),

cust_rpt_details as (
    select
        r.customer_id,
        r.avg_order_amount
    from {{ ref('cust_rpt') }} r
)

select
    ci.customer_id,
    ci.first_name,
    ci.last_name,
    od.first_order_date,
    od.most_recent_order_date,
    od.total_orders,
    coalesce(pd.lifetime_value, 0) as lifetime_value,
    coalesce(cr.avg_order_amount, 0) as avg_order_amount
from customer_info ci
left join order_details od
    on ci.customer_id = od.customer_id
left join payment_details pd
    on ci.customer_id = pd.customer_id
left join cust_rpt_details cr
    on ci.customer_id = cr.customer_id