WITH source AS (
SELECT *
FROM {{ source ('olist_raws', 'order_reviews') }}
),
clean AS (
SELECT
    review_id,
    order_id,
    review_score,
    COALESCE(review_comment_title, 'No Title') AS comment_title,
    COALESCE(review_comment_message, 'No Comment') AS comment_message,
    review_creation_date AS created_at,
    review_answer_timestamp AS answered_at
FROM source
QUALIFY ROW_NUMBER() OVER 
    (PARTITION BY review_id ORDER BY review_creation_date DESC) = 1
)

SELECT *
FROM clean
ORDER BY review_score



-- เช็คค่าซ้ำของ review_id ว่ามีหรือไม่ (ควรจะไม่มี)
-- SELECT review_id, COUNT(*) AS duplicate_count
-- FROM clean
-- GROUP BY review_id
-- HAVING COUNT(*) > 1