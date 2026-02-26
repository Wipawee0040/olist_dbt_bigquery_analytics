{% snapshot snapshot_orders %}

{{
    config(
      target_schema='snapshots',
      unique_key='order_id',
      
      strategy='check',
      check_cols=['order_status']
    )
}}

SELECT * FROM {{ source('olist_raws', 'orders') }}

{% endsnapshot %}