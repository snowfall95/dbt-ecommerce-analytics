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

orders as (

    select * from {{ ref('staging_orders') }}
    /* In this project, incremental_load will help ensure that only new or updated orders are processed during incremental runs.
       When incremental loading is enabled, this filter ensures we only process
       orders that have been updated since the last run. Uncomment the line below to enable it.

    {{ incremental_load('order_id', 'order_purchase_timestamp') }}
    */

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
    i.currency,
    sum(i.price) as total_price,
    sum(i.freight_value) as total_freight,
    sum(p.payment_value) as total_payment

from orders o
join items i on o.order_id = i.order_id
join payments p on o.order_id = p.order_id
group by 1,2,3,4,5,6,7,8,9


