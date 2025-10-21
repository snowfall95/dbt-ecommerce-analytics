with 

source as (

    select *
    from {{ source('brazillian_ecommerce', 'olist_geolocation_dataset') }}

),

transformed as (

    select
        geolocation_zip_code_prefix,
        geolocation_lat,
        geolocation_lng,
        geolocation_city,
        geolocation_state   
    from source

)

select * from transformed