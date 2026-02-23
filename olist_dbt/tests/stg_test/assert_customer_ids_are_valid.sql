-- tests/assert_customer_ids_are_valid.sql
WITH validation_errors AS (
    SELECT
        customer_id,
        customer_unique_id
    FROM {{ ref('stg_customers') }}
    WHERE
        NOT REGEXP_CONTAINS(customer_id, r'^[a-fA-F0-9]{32}$')
        OR
        NOT REGEXP_CONTAINS(customer_unique_id, r'^[a-fA-F0-9]{32}$')
)

SELECT * FROM validation_errors