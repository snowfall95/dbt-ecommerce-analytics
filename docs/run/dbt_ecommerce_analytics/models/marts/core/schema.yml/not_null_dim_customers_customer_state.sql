
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select customer_state
from `ecommerce-analytics-475706`.`dbt_ecommerce_core`.`dim_customers`
where customer_state is null



  
  
      
    ) dbt_internal_test