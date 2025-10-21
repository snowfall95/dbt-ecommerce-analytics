with

source as (

    select *
    from {{ source('brazillian_ecommerce', 'olist_customer_dataset') }}

),

transformed as (

    select
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state
    from source

)

select * from transformed