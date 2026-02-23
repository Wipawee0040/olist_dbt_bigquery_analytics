select
    order_id,
    created_at,
    delivered_at
from {{ ref('stg_orders') }}
where delivered_at < created_at