with source as (

    {#-
    Normally we would select from the table here, but we are using seeds to load
    our data in this project
    #}
    select * from {{ ref('i_cust_rpt') }}

),

renamed as (

    select
        id as customer_id,
       first_name,
       last_name,
       avg_order_amount

    from source

)

select * from renamed