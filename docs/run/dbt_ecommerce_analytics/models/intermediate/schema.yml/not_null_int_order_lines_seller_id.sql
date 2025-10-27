
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select seller_id
from `ecommerce-analytics-475706`.`dbt_ecommerce_intermediate`.`int_order_lines`
where seller_id is null



  
  
      
    ) dbt_internal_test