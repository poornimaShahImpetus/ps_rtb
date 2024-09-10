{% macro update_avg_order_amount() %}
    update {{ ref('cust_rpt') }} c
    set avg_order_amount = a.average_order_amount
    from {{ ref('average_order_amount_by_customer') }} a
    where c.customer_id = a.customer_id;

{% endmacro %}