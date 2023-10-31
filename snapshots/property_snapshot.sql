{% snapshot property_snapshot %}

{{
        config(
          target_schema='raw',
          strategy='timestamp',
          unique_key='LISTING_ID',
          updated_at='scraped_date',
        )
    }}

select DISTINCT LISTING_ID,scraped_date,PROPERTY_TYPE,LISTING_NEIGHBOURHOOD from {{ source('raw', 'listing_data') }}

{% endsnapshot %}

