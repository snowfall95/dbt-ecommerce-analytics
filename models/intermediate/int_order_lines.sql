{{ config(
    materialized='table'  
) }}

with

orders as (

    select * from {{ ref('staging_orders') }}

),

items as (

    select * from {{ ref('staging_order_items') }}

),

product_category as (

    select 
        product_id,
        product_category_name
    from {{ ref('staging_products') }}

),

category_translation as (

    select 
        product_category_name,
        product_category_name_english
    from {{ ref('product_category_name_translation') }} -- this is seed

)

select 

    i.order_id,
    i.order_item_id,
    i.product_id,
    ct.product_category_name_english,
    o.customer_id,
    i.seller_id,
    i.shipping_limit_date,
    i.currency,
    i.price,
    i.freight_value,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date

from items i
left join orders o 
on i.order_id = o.order_id
left join product_category pc
on i.product_id = pc.product_id
left join category_translation ct 
on pc.product_category_name = ct.product_category_name
