{{
    config(
        unique_key='listing_id'
    )
}}

select * FROM {{ ref('room_stg') }}

