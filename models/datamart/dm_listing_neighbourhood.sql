WITH active_listings AS (
    SELECT
        listing_neighbourhood,
        TO_CHAR(scraped_date, 'MM/YYYY') AS month_year,
        COUNT(*) AS total_active_listings
    FROM
        {{ ref('facts_listings') }}
    WHERE
        has_availability = 't'
    GROUP BY
        listing_neighbourhood, month_year
),
inactive_listings AS (
    SELECT
        listing_neighbourhood,
        TO_CHAR(scraped_date, 'MM/YYYY') AS month_year,
        COUNT(*) AS total_inactive_listings
    FROM
        {{ ref('facts_listings') }}
    WHERE
        has_availability = 'f'
    GROUP BY
        listing_neighbourhood, month_year
),
all_listings AS (
    SELECT
        listing_neighbourhood,
        TO_CHAR(scraped_date, 'MM/YYYY') AS month_year,
        COUNT(DISTINCT listing_id) AS total_listings
    FROM
        {{ ref('facts_listings') }}
    GROUP BY
        listing_neighbourhood, month_year
),
superhost_stats AS (
    SELECT
        listing_neighbourhood,
        TO_CHAR(scraped_date, 'MM/YYYY') AS month_year,
        COUNT(DISTINCT host_id) AS total_distinct_hosts,
        COUNT(DISTINCT CASE WHEN host_is_superhost = 't' THEN host_id ELSE NULL END) AS superhosts
    FROM
        {{ ref('facts_listings') }}
    GROUP BY
        listing_neighbourhood, month_year
),
price_stats AS (
    SELECT
        TO_CHAR(scraped_date, 'MM/YYYY') AS month_year,
        listing_neighbourhood,
        MIN(price) AS min_price,
        MAX(price) AS max_price,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY price) AS median_price,
        AVG(price) AS avg_price
    FROM
        {{ ref('facts_listings') }}
    GROUP BY
        listing_neighbourhood, month_year
),
revenue_stats AS (
    SELECT
        listing_neighbourhood,
        TO_CHAR(scraped_date, 'MM/YYYY') AS month_year,
        COUNT(*) AS total_stays,
        SUM(price * (30 - availability_30)) AS total_revenue
    FROM
        {{ ref('facts_listings') }}
    WHERE
        has_availability = 't'
    GROUP BY
        listing_neighbourhood, month_year
),
review_score as (
	select listing_neighbourhood,TO_CHAR(scraped_date, 'MM/YYYY') AS month_year,AVG(review_scores_rating) as avg_review_scores_rating 
    from  {{ ref('facts_listings') }}
	GROUP BY
        listing_neighbourhood, month_year
)
SELECT
    a.listing_neighbourhood,
    a.month_year,
    (COALESCE(total_active_listings, 0)/total_listings)*100 AS active_listing_rate,
    COALESCE(ps.min_price, 0) AS min_price,
    COALESCE(ps.max_price, 0) AS max_price,
    COALESCE(ps.median_price, 0) AS median_price,
    COALESCE(ps.avg_price, 0) AS avg_price,
    COALESCE(total_distinct_hosts, 0) AS total_distinct_hosts,
    (COALESCE(superhosts, 0)/total_distinct_hosts)*100 AS superhost_rate,
    rs2.avg_review_scores_rating AS avg_review_scores_rating,
    coalesce((al.total_active_listings - LAG(al.total_active_listings) OVER (ORDER BY TO_DATE(a.month_year, 'MM/YYYY'))) / NULLIF(LAG(al.total_active_listings) OVER (ORDER BY TO_DATE(a.month_year, 'MM/YYYY')), 0) * 100,0) AS percentage_change_active_listings,
    coalesce((il.total_inactive_listings - LAG(il.total_inactive_listings) OVER (ORDER BY TO_DATE(a.month_year, 'MM/YYYY'))) / NULLIF(LAG(il.total_inactive_listings) OVER (ORDER BY TO_DATE(a.month_year, 'MM/YYYY')), 0) * 100,0) AS percentage_change_inactive_listings,
    COALESCE(total_stays, 0) AS total_stays,
    CASE
        WHEN total_active_listings = 0 THEN 0
        ELSE AVG(total_revenue / total_active_listings)
    END AS avg_revenue_per_active_listings
from
	all_listings a
LEFT JOIN active_listings al ON a.listing_neighbourhood = al.listing_neighbourhood AND a.month_year = al.month_year 
LEFT JOIN inactive_listings il ON a.listing_neighbourhood = il.listing_neighbourhood AND a.month_year = il.month_year 
LEFT JOIN superhost_stats ss ON a.listing_neighbourhood = ss.listing_neighbourhood AND a.month_year = ss.month_year 
LEFT JOIN price_stats ps ON a.listing_neighbourhood = ps.listing_neighbourhood AND a.month_year = ps.month_year 
LEFT JOIN revenue_stats rs ON a.listing_neighbourhood = rs.listing_neighbourhood AND a.month_year = rs.month_year 
LEFT JOIN review_score rs2 ON a.listing_neighbourhood = rs2.listing_neighbourhood AND a.month_year = rs2.month_year 
group by a.listing_neighbourhood,a.month_year,al.total_active_listings,il.total_inactive_listings,a.total_listings,ss.superhosts,ss.total_distinct_hosts,ss.total_distinct_hosts,rs.total_stays,rs.total_revenue,ps.min_price,ps.max_price,ps.median_price,ps.avg_price,rs2.avg_review_scores_rating
