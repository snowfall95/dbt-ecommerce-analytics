
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select first_purchase
from `ecommerce-analytics-475706`.`dbt_ecommerce_mart`.`fact_customer_lifetime_value`
where first_purchase is null



  
  
      
    ) dbt_internal_test