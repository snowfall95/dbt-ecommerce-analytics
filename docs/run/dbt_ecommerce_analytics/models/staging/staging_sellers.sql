

  create or replace view `ecommerce-analytics-475706`.`dbt_ecommerce_staging`.`staging_sellers`
  OPTIONS()
  as with 

source as (

    select *
    from `ecommerce-analytics-475706`.`dbt_ecommerce_raw`.`olist_sellers_dataset`

),

transformed as (

    select
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state
    from source

)

select * from transformed;

