SELECT 
    product_id,
    product_category_name,
    weight_g,
    length_cm,
    height_cm,
    width_cm
FROM {{ ref('stg_product') }}