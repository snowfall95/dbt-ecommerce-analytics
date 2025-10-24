with 

source as (

    select *
    from {{ source('brazillian_ecommerce', 'olist_order_reviews_dataset') }}

),

transformed as (

    select
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date,
        review_answer_timestamp
    from source

)

select * from transformed