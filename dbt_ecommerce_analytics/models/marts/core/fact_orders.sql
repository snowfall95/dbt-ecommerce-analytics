with 

orders as (

    select * from {{ ref('staging_orders') }}

),

items as (

    select * from {{ ref('staging_order_items') }}

),

payments as (

    select * from {{ ref('staging_order_payments') }}

)

select 

    o.order_id,
    o.customer_id,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    sum(i.price) as total_price,
    sum(i.freight_value) as total_freight,
    sum(p.payment_value) as total_payment,

from orders o
join items i on o.order_id = i.order_id
join payments p on o.order_id = p.order_id
group by 1,2,3,4,5,6,7,8
