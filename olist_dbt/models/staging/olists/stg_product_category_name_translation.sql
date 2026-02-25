WITH source AS(
SELECT 
    string_field_0 AS product_category_name,
    string_field_1 AS product_category_name_english
FROM {{ source( 'olist_raws', 'product_category_name_translation')}}
WHERE string_field_0 != 'product_category_name_english'
),

clean AS (
SELECT 
    product_category_name,
    product_category_name_english
FROM source
ORDER BY product_category_name
)

SELECT *
FROM clean