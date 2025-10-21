with

customers as (

    select * from {{ ref('staging_customers') }}

), 

geo as (

    select * from {{ ref('staging_geolocation') }}

)

select 

    c.customer_id,
    c.customer_unique_id, 
    g.geolocation_state as state,
    g.geolocation_city as city,
    g.geolocation_zip_code_prefix as location_zip_code

from customers c
join geo g 
on c.customer_zip_code_prefix = g.geolocation_zip_code_prefix