WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

order_payments_summary AS (
    SELECT 
        order_id,
        MAX(payment_type) AS payment_type, 
        SUM(payment_value) AS payment_value 
    FROM {{ ref('stg_order_payments') }}
    GROUP BY 1
),

order_join AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.status,
        o.created_at,
        o.approved_at,
        o.shipped_at,
        o.estimated_delivery_at,
        -- เปลี่ยนจาก op. เป็น p. ให้หมดครับ!
        COALESCE(p.payment_type, 'Other') AS payment_type,
        COALESCE(p.payment_value, 0) AS payment_value
    FROM orders o
    -- ตรงนี้คุณตั้งชื่อว่า p แล้ว dbt เลยจะไปดึงข้อมูลที่ GROUP BY แล้วมาใช้ครับ
    LEFT JOIN order_payments_summary p ON o.order_id = p.order_id
    )

SELECT * FROM order_join