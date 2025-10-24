-- Granularity: Order Level / one row per order. 

/* I also applied macro incremental_load so that this fact table can be built incrementally. 
   In order to enable the incremental load to work, do the following:
   1. Uncomment the config block below
   2. Uncomment the incremental_load macro in the orders CTE
   
{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}
*/

with 

order_lines as (

    select * from {{ ref('int_order_lines') }}
    /* In this project, incremental_load will help ensure that only new or updated orders are processed during incremental runs.
       When incremental loading is enabled, this filter ensures we only process
       orders that have been updated since the last run. Uncomment the line below to enable it.

    {{ incremental_load('order_id', 'order_purchase_timestamp') }}
    */

),

payments as (

    select * from {{ ref('staging_order_payments') }}

),

payment_aggregated as (

    select

        order_id,
        sum(payment_value) as total_payment,
        count(distinct payment_type) as payment_method_used
    
    from payments 
    group by order_id

)

select 

    ol.order_id,
    ol.customer_id,
    ol.order_status,
    ol.order_purchase_timestamp,
    ol.order_approved_at,
    ol.order_delivered_carrier_date,
    ol.order_delivered_customer_date,
    ol.order_estimated_delivery_date,
    ol.currency,

    -- Aggregated Metrics
    count(distinct ol.order_item_id) as total_items,
    count(distinct ol.product_id) as unique_products,
    count(distinct ol.seller_id) as unique_sellers,
    round(sum(ol.price) + sum(ol.freight_value), 2) as order_gross_value,
    round(sum(ol.price), 2) as order_total_price,
    round(sum(ol.freight_value), 2) as order_total_freight,

    -- Payment Information
    pa.total_payment,
    pa.payment_method_used,

    -- Calculated Delivery Metrics
    date_diff(ol.order_purchase_timestamp, ol.order_delivered_customer_date, day) as delivery_days,
    date_diff(ol.order_purchase_timestamp, ol.order_approved_at, day) as purchase_to_approval_days,
    date_diff(ol.order_approved_at, ol.order_delivered_customer_date, day) as approval_to_delivery_days,
    date_diff(ol.order_delivered_customer_date, ol.order_estimated_delivery_date, day) as delivery_variance_days

from order_lines ol
join payment_aggregated pa on ol.order_id = pa.order_id
group by 1,2,3,4,5,6,7,8,9,16,17 


