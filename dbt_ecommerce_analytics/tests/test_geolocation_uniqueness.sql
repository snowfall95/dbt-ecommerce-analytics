select 
    geolocation_zip_code_prefix,
    count(*) as count_duplicates
from {{ ref('staging_geolocation') }}
group by 1
having count(*) > 1 
