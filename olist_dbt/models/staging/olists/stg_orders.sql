WITH source AS 
(SELECT *
FROM {{ source('olist_raws' , 'orders') }}
),
clean AS
(SELECT
    order_id,
    customer_id,
    order_status as status,
    order_purchase_timestamp as created_at,
    order_approved_at as approved_at,
    order_delivered_carrier_date as shipped_at,
    order_delivered_customer_date as delivered_at,
    order_estimated_delivery_date as estimated_delivery_at
FROM source
)
SELECT *
FROM clean