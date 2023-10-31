{{
    config(
        unique_key='SUBURB_NAME'
    )
}}
SELECT coalesce(LGA_NAME,'Unknown') as LGA_NAME,
    coalesce(SUBURB_NAME,'Unknown') as SUBURB_NAME
FROM "postgres"."raw"."lgas_suburb"
