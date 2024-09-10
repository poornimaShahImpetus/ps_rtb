{% macro unload_to_stage(stage_name) %}
  {% set models = [
    'customer_order',
    'total_payment_for_each_order',
    'customer_high_total_payments',
    'order_with_many_payments_methods',
    'average_order_amount_by_customer'
  ] %}

  {% for model in models %}
    {% set model_ref = ref(model) %}
    {% set stage_path = stage_name ~ '/' ~ model ~ '.csv' %}

    -- Unload data from the model to the specified stage
    copy into @{{ stage_name }}/{{ model }}.csv
    from (select * from {{ model_ref }})
    file_format = (type = 'csv' field_delimiter = ',' skip_header = 1);

    {% if not loop.last %}
    -- Separate the commands with a newline for readability
    \n
    {% endif %}
  {% endfor %}
{% endmacro %}