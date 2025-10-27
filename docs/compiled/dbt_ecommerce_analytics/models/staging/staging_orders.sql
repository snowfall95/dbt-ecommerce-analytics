with 

source as (

    select *
    from `ecommerce-analytics-475706`.`dbt_ecommerce_raw`.`olist_orders_dataset`

),

transformed as (

    select
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date
    from source

)

select * from transformed