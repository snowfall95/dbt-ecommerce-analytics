
    
    

with all_values as (

    select
        delivery_performance_category as value_field,
        count(*) as n_records

    from `ecommerce-analytics-475706`.`dbt_ecommerce_mart`.`fact_delivery_performance`
    group by delivery_performance_category

)

select *
from all_values
where value_field not in (
    'Early','On-time','Slightly Late','Very Late'
)


