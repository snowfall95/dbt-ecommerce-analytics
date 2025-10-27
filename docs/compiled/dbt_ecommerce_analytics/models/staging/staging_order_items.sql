with 

source as (

    select *
    from `ecommerce-analytics-475706`.`dbt_ecommerce_raw`.`olist_order_items_dataset`

),

transformed as (

    select
        order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_date,
        price,
        'BRL' as currency,
        freight_value   
    from source

)

select * from transformed