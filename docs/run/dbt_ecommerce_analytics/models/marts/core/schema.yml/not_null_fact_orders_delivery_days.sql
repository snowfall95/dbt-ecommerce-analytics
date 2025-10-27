
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select delivery_days
from `ecommerce-analytics-475706`.`dbt_ecommerce_core`.`fact_orders`
where delivery_days is null



  
  
      
    ) dbt_internal_test