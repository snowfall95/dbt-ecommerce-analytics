
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_reviews
from `ecommerce-analytics-475706`.`dbt_ecommerce_mart`.`fact_customer_satisfaction`
where total_reviews is null



  
  
      
    ) dbt_internal_test