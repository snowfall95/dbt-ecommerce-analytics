

  create or replace view `ecommerce-analytics-475706`.`dbt_ecommerce_staging`.`staging_order_payments`
  OPTIONS()
  as with 

source as (

    select *
    from `ecommerce-analytics-475706`.`dbt_ecommerce_raw`.`olist_order_payments_dataset`

),

transformed as (

    select
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value   
    from source

)

select * from transformed;

