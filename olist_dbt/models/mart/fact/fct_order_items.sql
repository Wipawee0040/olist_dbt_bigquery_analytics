{{
    config(
        materialized='incremental',
        unique_key='order_id'
    )
}}

WITH orders AS
(SELECT *
FROM {{ ref ('stg_orders') }}),
order_items AS
(SELECT *
FROM {{ ref ('stg_order_items') }}
),
join_orders AS
(SELECT
    o.customer_id, 
    o.order_id,
    oi.order_item_id,
    oi.product_id,
    oi.seller_id,
    o.status,
    oi.price,
    oi.freight_value
FROM orders o
INNER JOIN order_items oi
ON o.order_id = oi.order_id
ORDER BY o.order_id, oi.order_item_id
)

SELECT *
FROM join_orders

{% if is_incremental() %}
WHERE order_id > (SELECT MAX(order_id) FROM {{ this }})
{% endif %}