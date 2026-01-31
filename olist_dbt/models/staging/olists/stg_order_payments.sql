WITH source AS (
SELECT *
FROM {{ source('olist_raws', 'order_payments') }}
),
clean AS (
SELECT
    order_id,
    payment_sequential,
    CASE
         WHEN payment_type = 'credit_card' THEN 'Credit Card'
         WHEN payment_type = 'debit_card' THEN 'Debit Card'
         WHEN payment_type = 'voucher' THEN 'Voucher'
         WHEN payment_type = 'boleto' THEN 'Boleto'
         ELSE 'Other'
    END AS payment_type,
    CAST(payment_installments AS INTEGER) AS payment_installments,
    ROUND(payment_value, 2) AS payment_value
FROM source
)
SELECT *
FROM clean
ORDER BY payment_sequential