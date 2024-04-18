{{ config(materialized='view') }}

WITH filtered_data AS (
    SELECT 
        id,
        title,
        score,
        num_comments,
        author,
        created_utc,
        url,
        upvote_ratio,
        locked
    FROM {{ source('staging', 'worldnews_data') }}
),

deduplicated_data AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_utc DESC) AS rn
    FROM filtered_data
)

SELECT
    id,
    title,
    score,
    num_comments,
    author,
    created_utc,
    url,
    upvote_ratio,
    locked
FROM deduplicated_data
WHERE rn = 1
