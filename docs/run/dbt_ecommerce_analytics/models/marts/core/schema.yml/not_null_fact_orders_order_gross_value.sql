
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select order_gross_value
from `ecommerce-analytics-475706`.`dbt_ecommerce_core`.`fact_orders`
where order_gross_value is null



  
  
      
    ) dbt_internal_test