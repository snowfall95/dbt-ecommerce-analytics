with 

reviews as (

    select

        order_id,
        review_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date,
        review_answer_timestamp

    from {{ ref('staging_order_reviews') }}

), 

orders as (

    select

        order_id,
        customer_id,
        order_purchase_timestamp,
        order_delivered_customer_date,
        order_estimated_delivery_date

    from {{ ref('fact_orders') }}

)

select 

    o.customer_id,
    count(r.review_id) as total_reviews,
    round(avg(r.review_score), 2) as average_review_score,
    sum(case when r.review_score >= 4 then 1 else 0 end) as positive_reviews,
    sum(case when r.review_score <= 2 then 1 else 0 end) as negative_reviews,
    round(avg(date_diff(r.review_answer_timestamp, r.review_creation_date, day)), 2) as average_response_time_days

from orders o
left join reviews r on o.order_id = r.order_id
group by 1
order by 2 desc, 3 desc

/* 
Insights / ideas to visualize in the dashboard:
1. Average Review Score Distribution: A histogram showing the distribution of average review scores across customers.
2. Top 10 Customers by Positive Reviews: A bar chart highlighting the top 10 customers with the highest number of positive reviews.
3. Negative Review Analysis: A pie chart representing the proportion of negative reviews (scores 1 and 2) versus positive reviews (scores 4 and 5).
4. Average Response Time Trend: A line chart illustrating the trend of average response times to reviews over months.
5. Sellers with the best / worst customer satisfaction: A ranked list or bar chart showing sellers with the highest and lowest average review scores.
*/