with 

products as (

    select * from {{ ref('staging_products') }}

),

product_english_translations as (

    select * from {{ ref('product_category_name_translation') }}

)

select 

    p.product_id,
    p.product_category_name,
    pt.product_category_name_english,

from products p 
join product_english_translations pt
on p.product_category_name = pt.product_category_name