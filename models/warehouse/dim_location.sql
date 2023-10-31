{{
    config(
        unique_key='lga_code'
    )
}}
WITH lga_suburb AS (
    SELECT
        LGA_NAME,
        SUBURB_NAME
    FROM
        {{ ref('suburb_stg') }}
),
lga_combine AS (
    SELECT
        lc.lga_code AS lga_code,
        lc.LGA_NAME AS LGA_NAME,
        ls.SUBURB_NAME AS suburb_name
    FROM
        {{ ref('lga_stg') }} lc
    LEFT JOIN
        lga_suburb ls
    ON
        LOWER(lc.LGA_NAME) = LOWER(ls.LGA_NAME)
)
SELECT *
FROM lga_combine






