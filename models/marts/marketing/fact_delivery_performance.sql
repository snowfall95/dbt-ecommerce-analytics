-- Granularity: One row per delivered order with delivery metrics 

with

orders as (

    select 

        *

    from {{ ref('fact_orders') }}
    where order_delivered_customer_date is not null
      and order_estimated_delivery_date is not null
      and order_status = 'delivered'

), 

customers as (

    select * from {{ ref('dim_customers') }}

)


select 

    o.order_id,
    o.customer_id,
    c.state,
    c.city,

    -- Dates
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,

    -- Delivery Performance Metrics
    o.delivery_days,
    o.delivery_variance_days,

    -- Performance flags
    case 
        when o.delivery_variance_days < 0 then 'Early'
        when o.delivery_variance_days = 0 then 'On Time'
        when o.delivery_variance_days between 1 and 3 then 'Slightly Late'
        else 'Very Late'
    end as delivery_performance_category,

    case 
        when o.delivery_days <= 7 then 'Fast Delivery'
        when o.delivery_days <= 14 then 'Standard Delivery'
        else 'Slow Delivery'
    end as delivery_speed_category,

    -- Order Details
    o.total_items,
    o.unique_sellers,
    o.order_gross_value,
    o.total_payment,

    -- Calculate days between key milestones
    date_diff(o.order_purchase_timestamp, o.order_approved_at, day) as purchase_to_approval_days,
    date_diff(o.order_approved_at, o.order_delivered_carrier_date, day) as approval_to_carrier_days,
    date_diff(o.order_delivered_carrier_date, o.order_delivered_customer_date, day) as carrier_to_customer_days,
    date_diff(o.order_purchase_timestamp, o.order_delivered_customer_date, day) as purchase_to_delivery_days

from orders o
join customers c 
on o.customer_id = c.customer_id
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21

