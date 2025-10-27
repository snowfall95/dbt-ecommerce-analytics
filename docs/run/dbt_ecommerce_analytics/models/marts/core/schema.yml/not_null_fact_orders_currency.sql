
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select currency
from `ecommerce-analytics-475706`.`dbt_ecommerce_core`.`fact_orders`
where currency is null



  
  
      
    ) dbt_internal_test