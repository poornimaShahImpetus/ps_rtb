with source as (

    {#-
    Normally we would select from the table here, but we are using seeds to load
    our data in this project
    #}
    select * from {{ ref('r_payments') }}

),

renamed as (

    select
        id as payment_id,
        order_id,
        payment_method,
        amount

    from source

)

select * from renamed