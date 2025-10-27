
    
    



select order_estimated_delivery_date
from `ecommerce-analytics-475706`.`dbt_ecommerce_intermediate`.`int_order_lines`
where order_estimated_delivery_date is null


