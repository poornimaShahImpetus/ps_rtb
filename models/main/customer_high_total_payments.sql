with customer_payments as (
    select
        o.customer_id,
        sum(p.amount) as total_payment_amount
    from {{ ref('stg_orders') }} o
    join {{ ref('stg_payments') }} p
        on o.order_id = p.order_id
    group by o.customer_id
),

ranked_customers as (
    select
        cp.customer_id,
        c.first_name,
        c.last_name,
        cp.total_payment_amount,
        rank() over (order by cp.total_payment_amount desc) as payment_rank
    from customer_payments cp
    join {{ ref('stg_customers') }} c
        on cp.customer_id = c.customer_id
)

select
    customer_id,
    first_name,
    last_name,
    total_payment_amount,
    payment_rank
from ranked_customers
where payment_rank <= 10 
