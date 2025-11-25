{% snapshot stock_prices_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='symbol || date',
      strategy='check',
      check_cols=['close', 'volume'],
    )
}}

SELECT
    symbol,
    date,
    open,
    high,
    low,
    close,
    volume
FROM {{ source('raw', 'stock_data_lab') }}

{% endsnapshot %}