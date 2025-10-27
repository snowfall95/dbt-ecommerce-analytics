select 
    geolocation_zip_code_prefix,
    count(*) as count_duplicates
from `ecommerce-analytics-475706`.`dbt_ecommerce_staging`.`staging_geolocation`
group by 1
having count(*) > 1