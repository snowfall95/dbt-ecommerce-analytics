with 

source as (

    select *
    from {{ source('brazillian_ecommerce', 'olist_order_payments_dataset') }}

),

transformed as (

    select
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value   
    from source

)

select * from transformed