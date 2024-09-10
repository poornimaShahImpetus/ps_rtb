with order_info as (
    select
        o.order_id,
        o.customer_id,
        o.order_date,
        o.status,
        sum(p.amount) as total_amount
    from {{ ref('stg_orders') }} o
    left join {{ ref('stg_payments') }} p
        on o.order_id = p.order_id
    group by o.order_id, o.customer_id, o.order_date, o.status
),

payment_method_details as (
    select
        o.order_id,
        sum(case when p.payment_method = 'credit_card' then p.amount else 0 end) as credit_card_amount,
        sum(case when p.payment_method = 'coupon' then p.amount else 0 end) as coupon_amount,
        sum(case when p.payment_method = 'bank_transfer' then p.amount else 0 end) as bank_transfer_amount,
        sum(case when p.payment_method = 'gift_card' then p.amount else 0 end) as gift_card_amount
    from {{ ref('stg_orders') }} o
    join {{ ref('stg_payments') }} p
        on o.order_id = p.order_id
    group by o.order_id
)

select
    oi.order_id,
    oi.customer_id,
    oi.order_date,
    oi.status,
    oi.total_amount,
    coalesce(pmd.credit_card_amount, 0) as credit_card_amount,
    coalesce(pmd.coupon_amount, 0) as coupon_amount,
    coalesce(pmd.bank_transfer_amount, 0) as bank_transfer_amount,
    coalesce(pmd.gift_card_amount, 0) as gift_card_amount
from order_info oi
left join payment_method_details pmd
    on oi.order_id = pmd.order_id