
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  select 
    geolocation_zip_code_prefix,
    count(*) as count_duplicates
from `ecommerce-analytics-475706`.`dbt_ecommerce_staging`.`staging_geolocation`
group by 1
having count(*) > 1
  
  
      
    ) dbt_internal_test