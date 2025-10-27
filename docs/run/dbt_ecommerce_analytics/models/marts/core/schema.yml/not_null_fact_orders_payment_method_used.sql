
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select payment_method_used
from `ecommerce-analytics-475706`.`dbt_ecommerce_core`.`fact_orders`
where payment_method_used is null



  
  
      
    ) dbt_internal_test