WITH lga_mapping AS (
    SELECT DISTINCT
        UPPER(LGA_NAME) AS lga_name,
        UPPER(SUBURB_NAME) AS suburb_name
    FROM {{ ref('dim_location') }} dl
),
transformed_host_data AS (
    SELECT
        t.*,
        COALESCE(lga.lga_name, 'unknown') AS host_neighbourhood_lga
    FROM {{ ref('facts_listings') }} t
    LEFT JOIN lga_mapping lga ON UPPER(t.host_neighbourhood) = lga.suburb_name
),
revenue_data AS (
    SELECT
        host_neighbourhood_lga,
        TO_CHAR(scraped_date, 'MM/YYYY') AS month_year,
        COUNT(DISTINCT host_id) AS distinct_hosts,
        SUM(price * (30 - availability_30)) AS total_revenue
    FROM transformed_host_data
    WHERE has_availability = 't'
    GROUP BY host_neighbourhood_lga, month_year
)
SELECT
    rd.host_neighbourhood_lga,
    rd.month_year,
    COALESCE(distinct_hosts, 0) AS num_distinct_hosts,
    COALESCE(total_revenue, 0) AS estimated_revenue,
    CASE
        WHEN distinct_hosts > 0 THEN total_revenue / distinct_hosts
        ELSE 0
    END AS estimated_revenue_per_host
FROM revenue_data rd
ORDER BY rd.host_neighbourhood_lga, rd.month_year


