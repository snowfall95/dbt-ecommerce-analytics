
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_payment
from `ecommerce-analytics-475706`.`dbt_ecommerce_core`.`fact_orders`
where total_payment is null



  
  
      
    ) dbt_internal_test