
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  

with all_values as (

    select
        order_status as value_field

    from `ecommerce-analytics-475706`.`dbt_ecommerce_staging`.`staging_orders`
    

),
set_values as (

    select
        cast('delivered' as string) as value_field
    union all
    select
        cast('shipped' as string) as value_field
    union all
    select
        cast('canceled' as string) as value_field
    union all
    select
        cast('invoiced' as string) as value_field
    union all
    select
        cast('processing' as string) as value_field
    union all
    select
        cast('approved' as string) as value_field
    union all
    select
        cast('created' as string) as value_field
    
    
),
validation_errors as (
    -- values from the model that are not in the set
    select
        v.value_field
    from
        all_values v
        left join
        set_values s on v.value_field = s.value_field
    where
        s.value_field is null

)

select *
from validation_errors


  
  
      
    ) dbt_internal_test