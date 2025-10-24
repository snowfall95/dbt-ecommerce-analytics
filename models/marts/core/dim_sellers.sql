with 

sellers as (

    select * from {{ ref('staging_sellers') }}

),

geo as (

    select * from {{ ref('staging_geolocation') }}

)

select 

    s.seller_id,
    s.seller_zip_code_prefix,
    s.seller_city,
    s.seller_state

from sellers s
join geo g
on s.seller_zip_code_prefix = g.geolocation_zip_code_prefix
