WITH source AS
(SELECT *
FROM {{ source ('olist_raws', 'order_items') }})
, 
clean AS (
SELECT
    order_id,
    CAST(order_item_id AS STRING) AS order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    ROUND(price, 2) AS price,
    ROUND(freight_value, 2) AS freight_value
FROM source
ORDER BY order_id
)

SELECT *
FROM clean