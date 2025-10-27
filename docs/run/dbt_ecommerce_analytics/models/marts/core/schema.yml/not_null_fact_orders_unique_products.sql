
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select unique_products
from `ecommerce-analytics-475706`.`dbt_ecommerce_core`.`fact_orders`
where unique_products is null



  
  
      
    ) dbt_internal_test