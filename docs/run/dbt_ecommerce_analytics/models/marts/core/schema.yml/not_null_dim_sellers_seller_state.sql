
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select seller_state
from `ecommerce-analytics-475706`.`dbt_ecommerce_core`.`dim_sellers`
where seller_state is null



  
  
      
    ) dbt_internal_test