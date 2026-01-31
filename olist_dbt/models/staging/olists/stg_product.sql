WITH source AS (
SELECT *
FROM {{ source('olist_raws', 'product') }}
),
clean AS
(SELECT
    product_id,
    INITCAP(COALESCE(product_category_name, 'Other')) AS product_category_name,
    product_photos_qty as photos_qty,
    product_weight_g as weight_g,
    product_length_cm as length_cm,
    product_height_cm as height_cm,
    product_width_cm as width_cm
FROM source)

SELECT *
FROM clean
ORDER BY product_category_name