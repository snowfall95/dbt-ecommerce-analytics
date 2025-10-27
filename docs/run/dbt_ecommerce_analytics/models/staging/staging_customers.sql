

  create or replace view `ecommerce-analytics-475706`.`dbt_ecommerce_staging`.`staging_customers`
  OPTIONS()
  as with

source as (

    select *
    from `ecommerce-analytics-475706`.`dbt_ecommerce_raw`.`olist_customer_dataset`

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

select * from transformed;

