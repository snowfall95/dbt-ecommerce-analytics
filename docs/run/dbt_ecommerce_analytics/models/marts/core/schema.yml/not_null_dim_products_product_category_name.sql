
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select product_category_name
from `ecommerce-analytics-475706`.`dbt_ecommerce_core`.`dim_products`
where product_category_name is null



  
  
      
    ) dbt_internal_test