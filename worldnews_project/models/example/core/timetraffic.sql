{{ config(materialized='table') }}

SELECT
    EXTRACT(HOUR FROM created_utc) AS hour_of_day,
    COUNT(*) AS num_posts,
    SUM(num_comments) AS total_comments
FROM {{ ref('stg_reddit_worldnews') }}
GROUP BY hour_of_day
ORDER BY hour_of_day
