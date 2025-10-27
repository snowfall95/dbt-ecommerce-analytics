
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select average_review_score
from `ecommerce-analytics-475706`.`dbt_ecommerce_mart`.`fact_customer_satisfaction`
where average_review_score is null



  
  
      
    ) dbt_internal_test