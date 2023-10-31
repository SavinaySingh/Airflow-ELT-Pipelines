{% snapshot host_snapshot %}

{{
        config(
          target_schema='raw',
          strategy='timestamp',
          unique_key='HOST_ID',
          updated_at='scraped_date',
        )
    }}

select DISTINCT HOST_ID,scraped_date,HOST_NAME,HOST_SINCE,HOST_IS_SUPERHOST,HOST_NEIGHBOURHOOD from {{ source('raw', 'listing_data') }}

{% endsnapshot %}

