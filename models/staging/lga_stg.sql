{{
    config(
        unique_key='LGA_CODE'
    )
}}
SELECT  LGA_CODE,
coalesce(LGA_NAME,'Unknown') as LGA_NAME
 FROM "postgres"."raw"."lgas_code"
