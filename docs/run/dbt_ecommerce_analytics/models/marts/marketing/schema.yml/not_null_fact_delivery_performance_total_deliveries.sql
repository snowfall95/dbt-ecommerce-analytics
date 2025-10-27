
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_deliveries
from `ecommerce-analytics-475706`.`dbt_ecommerce_mart`.`fact_delivery_performance`
where total_deliveries is null



  
  
      
    ) dbt_internal_test