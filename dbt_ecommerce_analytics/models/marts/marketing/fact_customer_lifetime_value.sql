with 

orders as (

    select * from {{ ref('fact_orders') }}

),

customers as (

    select * from {{ ref('dim_customers') }}

)

select 

    c.customer_unique_id,
    count(distinct o.order_id) as total_orders,
    round(sum(o.total_payment), 2) as total_spent,
    round(avg(o.total_payment), 2) as average_order_value,
    min(o.order_purchase_timestamp) as first_purchase,
    max(o.order_purchase_timestamp) as recent_purchase,
    date_diff(max(o.order_purchase_timestamp), min(o.order_purchase_timestamp), day) as customer_lifespan_days

from customers c 
join orders o 
on c.customer_id = o.customer_id
group by 1
order by 3 desc


/* 
Insights / ideas to visualize in the dashboard:
1. Top 10 Customers by Total Spent: A bar chart showcasing the top 10 customers based on the total amount spent.
2. Average Order Value Distribution: A histogram displaying the distribution of average order values across all customers
3. Customer Lifespan Analysis: A line chart illustrating the average customer lifespan in days over different cohorts (e.g., by month of first purchase).
4. CLV per region/state/country: A map visualization showing average customer lifetime value segmented by geographic regions.
5. % of one-time vs repeat customers: A pie chart representing the proportion of one-time customers versus repeat customers based on their order history.
*/