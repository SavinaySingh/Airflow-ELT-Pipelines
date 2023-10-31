{{
    config(
        unique_key='LISTING_ID'
    )
}}

WITH DimensionCheck AS
(
    SELECT
        LISTING_ID,
        SCRAPED_DATE,
        CASE WHEN HOST_ID IN (SELECT DISTINCT HOST_ID FROM {{ ref('host_stg') }}) THEN HOST_ID ELSE 0 END AS HOST_ID,
        ACCOMMODATES,
        PRICE,
        HAS_AVAILABILITY,
        AVAILABILITY_30,
        NUMBER_OF_REVIEWS,
        REVIEW_SCORES_RATING,
        REVIEW_SCORES_ACCURACY,
        REVIEW_SCORES_CLEANLINESS,
        REVIEW_SCORES_CHECKIN,
        REVIEW_SCORES_COMMUNICATION,
        REVIEW_SCORES_VALUE
    FROM {{ ref('final_facts_stg') }}
)
SELECT
    A.LISTING_ID,
    A.SCRAPED_DATE,
    B.HOST_ID,
    C.listing_neighbourhood,
    C.property_type,
    D.room_type,
    B.host_name,
    B.host_since,
    B.host_is_superhost,
    B.host_neighbourhood,
    A.ACCOMMODATES,
    A.PRICE,
    A.HAS_AVAILABILITY,
    A.AVAILABILITY_30,
    A.NUMBER_OF_REVIEWS,
    A.REVIEW_SCORES_RATING,
    A.REVIEW_SCORES_ACCURACY,
    A.REVIEW_SCORES_CLEANLINESS,
    A.REVIEW_SCORES_CHECKIN,
    A.REVIEW_SCORES_COMMUNICATION,
    A.REVIEW_SCORES_VALUE
FROM DimensionCheck A
LEFT JOIN staging.host_stg B ON A.HOST_ID = B.HOST_ID AND A.SCRAPED_DATE BETWEEN B.dbt_valid_from AND B.dbt_valid_to
LEFT JOIN staging.property_stg C ON A.LISTING_ID = C.LISTING_ID AND A.SCRAPED_DATE BETWEEN C.dbt_valid_from AND C.dbt_valid_to
LEFT JOIN staging.room_stg D ON A.LISTING_ID = D.LISTING_ID AND A.SCRAPED_DATE BETWEEN D.dbt_valid_from AND D.dbt_valid_to