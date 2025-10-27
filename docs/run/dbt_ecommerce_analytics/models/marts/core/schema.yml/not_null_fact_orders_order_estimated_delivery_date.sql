
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select order_estimated_delivery_date
from `ecommerce-analytics-475706`.`dbt_ecommerce_core`.`fact_orders`
where order_estimated_delivery_date is null



  
  
      
    ) dbt_internal_test