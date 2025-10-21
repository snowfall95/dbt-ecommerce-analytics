with

orders as (

    select 

        order_id,
        customer_id,
        order_delivered_customer_date,
        order_estimated_delivery_date,
        date_diff(order_delivered_customer_date, order_estimated_delivery_date, day) as delivery_delay_days

    from {{ ref('fact_orders') }}
    where order_delivered_customer_date is not null
      and order_estimated_delivery_date is not null

), 

delivery_performance as (

    select 

        customer_id,
        case 
            when delivery_delay_days < 0 then 'Early'
            when delivery_delay_days = 0 then 'On Time'
            when delivery_delay_days between 1 and 3 then 'Slightly Late'
            else 'Very Late'
        end as delivery_performance_category,
        count(*) as total_deliveries,
        round(avg(delivery_delay_days), 2) as average_delay_days

    from orders
    group by 1, 2

)

select 

    o.customer_id,
    delivery_performance_category,
    total_deliveries,
    average_delay_days
    
from orders o 
join delivery_performance d on o.customer_id = d.customer_id

/* 
Insights / ideas to visualize in the dashboard:
1. Average delay by state / seller region: A bar chart showing average delivery delays segmented by state or seller region.
2. % late deliveries over time: A line chart illustrating the trend of late deliveries over months.
3. Correlation with review scores: A scatter plot to analyze the correlation between delivery delays and customer review scores.
4. Delivery performance categories distribution: A pie chart representing the distribution of delivery performance categories (Early, On Time, Slightly Late, Very Late).
5. Top 10 customers with most late deliveries: A bar chart showcasing the top 10 customers who experienced the most late deliveries.
*/
