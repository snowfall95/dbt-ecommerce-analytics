{% {% snapshot customer_location_snapshot %}

{{
   config(
       target_database='target_database',
       target_schema='snapshots',
       unique_key='customer_unique_id',

       strategy='check',
       updated_at='updated_at',
   )
}}


{% endsnapshot %}

select 

    customer_unique_id,
    customer_zip_code_prefix,
    current_timestamp() as record_updated_at

from {{ ref('staging_customers') }}