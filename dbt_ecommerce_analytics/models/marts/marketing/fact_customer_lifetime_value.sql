-- Granularity: One row per customer with lifetime metrics

with 

orders as (

    select * from {{ ref('fact_orders') }}
    where order_status not in ('canceled', 'unavailable')

),

customers as (

    select * from {{ ref('dim_customers') }}

)

select 

    c.customer_id,
    c.customer_unique_id,
    c.city as customer_city,
    c.state as customer_state,

    -- Order Metrics
    count(distinct o.order_id) as total_orders,
    min(o.order_purchase_timestamp) as first_order_date,
    max(o.order_purchase_timestamp) as recent_order_date,
    date_diff(max(o.order_purchase_timestamp), min(o.order_purchase_timestamp), day) as customer_lifespan_days,

    -- Financial Metrics
    round(sum(o.order_gross_value), 2) as gross_revenue,
    round(sum(o.order_total_price), 2) as total_price,
    round(sum(o.order_total_freight), 2) as total_freight,
    round(sum(o.total_payment), 2) as total_spent,
    round(avg(o.total_payment), 2) as average_order_value,

    -- Product diversity
    count(distinct o.unique_products) as total_unique_products_ordered,
    avg(o.unique_products) as avg_unique_products_per_order,

    -- Recency Metrics
    date_diff(current_timestamp(), max(o.order_purchase_timestamp), day) as days_since_last_order,

    -- Customer segments
    case 
        when count(distinct o.order_id) = 1 then 'one-time'
        when count(distinct o.order_id) between 2 and 5 then 'repeat'
        else 'loyal'
    end as customer_segment,

    case 
        when date_diff(max(o.order_purchase_timestamp), min(o.order_purchase_timestamp), day) < 30 then 'new'
        when date_diff(max(o.order_purchase_timestamp), min(o.order_purchase_timestamp), day) between 30 and 180 then 'active'
        else 'dormant'
    end as customer_activity_status

from customers c 
left join orders o -- LEFT JOIN is to ensure we get all customers
    on c.customer_id = o.customer_id
group by 
    c.customer_id,
    c.customer_unique_id,
    c.city,
    c.state
order by gross_revenue desc

