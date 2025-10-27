
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select positive_reviews
from `ecommerce-analytics-475706`.`dbt_ecommerce_mart`.`fact_customer_satisfaction`
where positive_reviews is null



  
  
      
    ) dbt_internal_test