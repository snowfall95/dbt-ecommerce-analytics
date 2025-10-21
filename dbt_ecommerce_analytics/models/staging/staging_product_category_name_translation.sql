with 

source as (

    select *
    from {{ source('brazillian_ecommerce', 'product_category_name_translation') }}

),

transformed as (

    SELECT
        product_category_name,
        product_category_name_english
    FROM source


)

select * from transformed