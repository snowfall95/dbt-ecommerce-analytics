{% snapshot customer_location_snapshot %}

{{
   config (
       
       target_database='ecommerce-analytics-475706',
       target_schema='snapshots',
       unique_key='customer_unique_id',
       strategy='timestamp',
       updated_at = 'record_updated_at'

   )
}}

select 

    customer_unique_id,
    customer_zip_code_prefix,
    current_timestamp() as record_updated_at

from {{ ref('staging_customers') }}

{% endsnapshot %}
