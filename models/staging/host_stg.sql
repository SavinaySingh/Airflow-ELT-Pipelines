{{
    config(
        unique_key='HOST_ID'
    )
}}
with source  as (
        select
        to_date(coalesce(SCRAPED_DATE,'1900-01-01'),'YYYY-MM-DD') as SCRAPED_DATE,
        HOST_ID::int as HOST_ID,
        coalesce(HOST_NAME, 'Unknown') as HOST_NAME,
        case when HOST_SINCE = 'NaN' then to_date('01/01/1990','DD/MM/YYYY') else to_date(HOST_SINCE,'DD/MM/YYYY') end as HOST_SINCE,
        HOST_IS_SUPERHOST,
        case when HOST_NEIGHBOURHOOD = 'NaN' then 'Unknown' else HOST_NEIGHBOURHOOD end as host_neighbourhood,
        to_date(coalesce(dbt_valid_from,'1900-01-01'),'YYYY-MM-DD') as dbt_valid_from,
        to_date(coalesce(dbt_valid_to,'2100-10-01'),'YYYY-MM-DD') as dbt_valid_to
        from {{ ref('host_snapshot') }}
)
select * from source
