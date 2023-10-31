{{
    config(
        unique_key='LISTING_ID'
    )
}}
with source  as (

   select LISTING_ID,
        to_date(coalesce(SCRAPED_DATE,'1900-01-01'),'YYYY-MM-DD') as SCRAPED_DATE,
        coalesce(ROOM_TYPE,'Unknown') as ROOM_TYPE,
        to_date(coalesce(dbt_valid_from,'1900-01-01'),'YYYY-MM-DD') as dbt_valid_from,
        to_date(coalesce(dbt_valid_to,'2100-10-01'),'YYYY-MM-DD') as dbt_valid_to
    from {{ ref('room_snapshot') }}

)
select * from source