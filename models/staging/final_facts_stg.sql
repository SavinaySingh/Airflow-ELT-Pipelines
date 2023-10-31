{{
    config(
        unique_key='LISTING_ID'
    )
}}

select  LISTING_ID,
        to_date(coalesce(SCRAPED_DATE,'1900-01-01'),'YYYY-MM-DD') as SCRAPED_DATE,
        host_id,
        COALESCE(NULLIF(price, 'NaN')::integer, 0) as price,
        COALESCE(NULLIF(accommodates, 'NaN')::integer, 0) as accommodates,
        has_availability,
        COALESCE(NULLIF(availability_30, 'NaN')::integer, 0) as availability_30,
        COALESCE(NULLIF(number_of_reviews, 'NaN')::integer, 0) as number_of_reviews,
        COALESCE(NULLIF(review_scores_rating, 'NaN')::integer, 0) as review_scores_rating,
        COALESCE(NULLIF(review_scores_accuracy, 'NaN')::integer, 0) as review_scores_accuracy,
        COALESCE(NULLIF(review_scores_cleanliness, 'NaN')::integer, 0) as review_scores_cleanliness,
        COALESCE(NULLIF(review_scores_checkin, 'NaN')::integer, 0) as review_scores_checkin,
        COALESCE(NULLIF(review_Scores_communication, 'NaN')::integer, 0) as review_Scores_communication,
        COALESCE(NULLIF(review_scores_value, 'NaN')::integer, 0) as review_scores_value
        from "postgres"."raw"."listing_data"











