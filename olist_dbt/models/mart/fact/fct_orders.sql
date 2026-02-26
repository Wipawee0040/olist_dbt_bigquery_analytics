WITH orders AS
(SELECT *
FROM {{ ref ('stg_orders') }}),
order_payments AS
(SELECT 
    order_id,
    MAX(payment_type) AS payment_type, 
        -- รวมยอดเงินทั้งหมดที่จ่ายในออเดอร์นั้น
    SUM(payment_value) AS payment_value 
FROM {{ ref('stg_order_payments') }}
GROUP BY order_id
),
order_join AS
(SELECT
    o.order_id,
    o.customer_id,
    o.status,
    o.created_at,
    o.approved_at,
    o.shipped_at,
    o.estimated_delivery_at,
    COALESCE(op.payment_type, 'Other') AS payment_type,
    COALESCE(MAX(op.payment_value), 0) AS payment_value
FROM orders o
    LEFT JOIN order_payments op ON o.order_id = op.order_id
)

SELECT
*
FROM order_join