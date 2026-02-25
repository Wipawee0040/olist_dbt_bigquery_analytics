WITH source AS
(SELECT *
FROM {{ source('olist_raws', 'geolocation') }}
), 
clean AS (
SELECT
    CAST(geolocation_zip_code_prefix AS STRING) AS zip_code,
    ROUND(AVG(geolocation_lat), 4) AS latitude,
    ROUND(AVG(geolocation_lng), 4) AS longitude,
    MAX(TRIM(INITCAP(geolocation_city))) AS city,
    MAX(TRIM(UPPER(geolocation_state))) AS state
FROM source
GROUP BY 1
)

SELECT *
FROM clean
ORDER BY zip_code