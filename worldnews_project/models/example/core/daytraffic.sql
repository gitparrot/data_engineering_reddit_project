{{ config(materialized='table') }}

SELECT
    FORMAT_DATE('%A', DATE(created_utc)) AS day_of_week,
    COUNT(*) AS num_posts,
    AVG(score) AS avg_score,
    AVG(num_comments) AS avg_comments
FROM {{ ref('stg_reddit_worldnews') }}
GROUP BY day_of_week
ORDER BY avg_score DESC, avg_comments DESC
