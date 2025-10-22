/*
This staging table might seem a bit lengthy for staging, that is because i noticed that
the geolocation data has multiple entries for the same zip code prefix, for instance "sao paulo" and "são paulo" 
for the same zip code prefix. To ensure data consistency, i then implemented logic as shown below.
*/

with 

source as (

    select *
    from {{ source('brazillian_ecommerce', 'olist_geolocation_dataset') }}

),

city_counts as (

    select
        geolocation_zip_code_prefix,
        geolocation_city,
        geolocation_state,
        count(*) as occurrence_count
        
    from source
    group by 1, 2, 3

),

ranked as (

    select
        geolocation_zip_code_prefix,
        geolocation_city,
        geolocation_state,
        occurrence_count,
        
        row_number() over (
            partition by geolocation_zip_code_prefix
            order by occurrence_count desc, geolocation_city asc
        ) as row_num
        
    from city_counts

),

transformed as (

    select
        geolocation_zip_code_prefix,
        geolocation_city,
        geolocation_state
    from ranked
    where row_num = 1

)

select * from transformed