
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select order_purchase_timestamp
from `ecommerce-analytics-475706`.`dbt_ecommerce_intermediate`.`int_order_lines`
where order_purchase_timestamp is null



  
  
      
    ) dbt_internal_test