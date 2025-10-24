with 

orders as (

    select

        *

    from {{ ref('fact_orders') }}

),
reviews as (

    select

        *

    from {{ ref('staging_order_reviews') }}

)

select 

    o.customer_id,
    o.order_id,
    o.order_purchase_timestamp,
    o.order_status,

    -- Review metrics
    r.review_score,
    r.review_comment_title,

    -- Order context
    o.total_payment,
    o.total_items,

    -- Delivery metrics (calculate directly)
    o.delivery_days,
    o.delivery_variance_days,
    case
        when o.delivery_variance_days >= 0 then 'On Time'
        else 'Late'
    end as delivery_status,
    case
        when o.delivery_days <= 7 then 'Fast'
        when o.delivery_days between 8 and 14 then 'Average'
        else 'Slow'
    end as delivery_speed,

    -- Satisfaction 
    case 
        when r.review_score >= 4 then 'Satisfied'
        when r.review_score = 3 then 'Neutral'
        when r.review_score <= 2 then 'Dissatisfied'
        else 'No Review'
    end as satisfaction_category

/*
    count(r.review_id) as total_reviews,
    round(avg(r.review_score), 2) as average_review_score,
    sum(case when r.review_score >= 4 then 1 else 0 end) as positive_reviews,
    sum(case when r.review_score <= 2 then 1 else 0 end) as negative_reviews,
    round(avg(date_diff(r.review_answer_timestamp, r.review_creation_date, day)), 2) as average_response_time_days
*/

from orders o
left join reviews r on o.order_id = r.order_id
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
order by 2 desc, 3 desc
