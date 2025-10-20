with 

source as (

    select *
    from {{ source('brazillian_ecommerce', 'olist_order_items_dataset') }}

),

transformed as (

    select
        order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_date,
        price,
        freight_value   
    from source

)

select * from transformed