
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select unique_sellers
from `ecommerce-analytics-475706`.`dbt_ecommerce_core`.`fact_orders`
where unique_sellers is null



  
  
      
    ) dbt_internal_test