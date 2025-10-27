
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select average_order_value
from `ecommerce-analytics-475706`.`dbt_ecommerce_mart`.`fact_customer_lifetime_value`
where average_order_value is null



  
  
      
    ) dbt_internal_test