{% snapshot room_snapshot %}

{{  
        config(
          target_schema='raw',
          strategy='timestamp',
          unique_key='LISTING_ID',
          updated_at='scraped_date',
        )
    }}

select DISTINCT LISTING_ID,scraped_date,ROOM_TYPE from {{ source('raw', 'listing_data') }}

{% endsnapshot %}