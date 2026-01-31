WITH source AS 
(SELECT *
FROM {{ source('olist_raws' , 'sellers') }}
),
clean AS(
SELECT
    seller_id,
    LPAD(CAST(seller_zip_code_prefix AS STRING) , 5, '0') AS zip_code,
    CASE 
        WHEN REGEXP_CONTAINS(seller_city, r'^[0-9]+$') THEN 'Unknown'
        WHEN seller_city LIKE '%@%' OR seller_city LIKE '%.com%' THEN 'Unknown'
        ELSE INITCAP(TRIM(REGEXP_REPLACE(LOWER(seller_city), r'[\/-].*', '')))
    END AS city,
    seller_state AS state
FROM source
)

SELECT *
FROM clean
ORDER BY city