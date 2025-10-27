



with validation_errors as (

    select
        order_id,
        count(*) as `n_records`
    from `ecommerce-analytics-475706`.`dbt_ecommerce_staging`.`staging_orders`
    where
        1=1
        and 
    not (
        order_id is null
        
    )


    
    group by
        order_id
    having count(*) > 1

)
select * from validation_errors
