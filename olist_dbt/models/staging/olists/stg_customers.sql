WITH source AS 
(SELECT *
FROM {{ source('olist_raws' , 'customers') }}
),
clean AS
(SELECT
    customer_id,
    TRIM(customer_unique_id) AS customer_unique_id,
    TRIM(LPAD(CAST(customer_zip_code_prefix AS STRING),5, '0')) AS zip_code,
    TRIM(initcap(customer_city)) AS city,
    TRIM(UPPER(customer_state)) AS state
FROM source
)
SELECT *
FROM clean
