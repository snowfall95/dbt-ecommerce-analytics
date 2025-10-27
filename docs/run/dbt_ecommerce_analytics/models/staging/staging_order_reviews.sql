

  create or replace view `ecommerce-analytics-475706`.`dbt_ecommerce_staging`.`staging_order_reviews`
  OPTIONS()
  as with 

source as (

    select *
    from `ecommerce-analytics-475706`.`dbt_ecommerce_raw`.`olist_order_reviews_dataset`

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

select * from transformed;

