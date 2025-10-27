-- back compat for old kwarg name
  
  
        
            
	    
	    
            
        
    

    

    merge into `ecommerce-analytics-475706`.`dbt_ecommerce_core`.`fact_orders` as DBT_INTERNAL_DEST
        using (-- Granularity: Order Level / one row per order. 

/* I also applied macro incremental_load so that this fact table can be built incrementally. 
   In order to enable the incremental load to work, do the following:
   1. Uncomment the config block below
   2. Uncomment the incremental_load macro in the orders CTE
   

*/

with 

order_lines as (

    select * from `ecommerce-analytics-475706`.`dbt_ecommerce_intermediate`.`int_order_lines`
    /* In this project, incremental_load will help ensure that only new or updated orders are processed during incremental runs.
       When incremental loading is enabled, this filter ensures we only process
       orders that have been updated since the last run. Uncomment the line below to enable it.

    

  

    

    where order_purchase_timestamp > (select max(order_purchase_timestamp) from `ecommerce-analytics-475706`.`dbt_ecommerce_core`.`fact_orders`)

  


    */

),

payments as (

    select * from `ecommerce-analytics-475706`.`dbt_ecommerce_staging`.`staging_order_payments`

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
        ) as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.order_id = DBT_INTERNAL_DEST.order_id))

    
    when matched then update set
        `order_id` = DBT_INTERNAL_SOURCE.`order_id`,`customer_id` = DBT_INTERNAL_SOURCE.`customer_id`,`order_status` = DBT_INTERNAL_SOURCE.`order_status`,`order_purchase_timestamp` = DBT_INTERNAL_SOURCE.`order_purchase_timestamp`,`order_approved_at` = DBT_INTERNAL_SOURCE.`order_approved_at`,`order_delivered_carrier_date` = DBT_INTERNAL_SOURCE.`order_delivered_carrier_date`,`order_delivered_customer_date` = DBT_INTERNAL_SOURCE.`order_delivered_customer_date`,`order_estimated_delivery_date` = DBT_INTERNAL_SOURCE.`order_estimated_delivery_date`,`currency` = DBT_INTERNAL_SOURCE.`currency`,`total_items` = DBT_INTERNAL_SOURCE.`total_items`,`unique_products` = DBT_INTERNAL_SOURCE.`unique_products`,`unique_sellers` = DBT_INTERNAL_SOURCE.`unique_sellers`,`order_gross_value` = DBT_INTERNAL_SOURCE.`order_gross_value`,`order_total_price` = DBT_INTERNAL_SOURCE.`order_total_price`,`order_total_freight` = DBT_INTERNAL_SOURCE.`order_total_freight`,`total_payment` = DBT_INTERNAL_SOURCE.`total_payment`,`payment_method_used` = DBT_INTERNAL_SOURCE.`payment_method_used`,`delivery_days` = DBT_INTERNAL_SOURCE.`delivery_days`,`purchase_to_approval_days` = DBT_INTERNAL_SOURCE.`purchase_to_approval_days`,`approval_to_delivery_days` = DBT_INTERNAL_SOURCE.`approval_to_delivery_days`,`delivery_variance_days` = DBT_INTERNAL_SOURCE.`delivery_variance_days`
    

    when not matched then insert
        (`order_id`, `customer_id`, `order_status`, `order_purchase_timestamp`, `order_approved_at`, `order_delivered_carrier_date`, `order_delivered_customer_date`, `order_estimated_delivery_date`, `currency`, `total_items`, `unique_products`, `unique_sellers`, `order_gross_value`, `order_total_price`, `order_total_freight`, `total_payment`, `payment_method_used`, `delivery_days`, `purchase_to_approval_days`, `approval_to_delivery_days`, `delivery_variance_days`)
    values
        (`order_id`, `customer_id`, `order_status`, `order_purchase_timestamp`, `order_approved_at`, `order_delivered_carrier_date`, `order_delivered_customer_date`, `order_estimated_delivery_date`, `currency`, `total_items`, `unique_products`, `unique_sellers`, `order_gross_value`, `order_total_price`, `order_total_freight`, `total_payment`, `payment_method_used`, `delivery_days`, `purchase_to_approval_days`, `approval_to_delivery_days`, `delivery_variance_days`)


    