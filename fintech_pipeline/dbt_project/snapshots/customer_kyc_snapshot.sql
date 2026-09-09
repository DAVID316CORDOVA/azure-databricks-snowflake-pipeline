{% snapshot customer_kyc_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='customer_id',
      strategy='timestamp',
      updated_at='exported_at_utc_minus_5',
    )
}}

select
    customer_id,
    kyc_status,
    account_balance,
    exported_at_utc_minus_5
from {{ source('raw', 'customers_raw') }}
qualify row_number() over (
    partition by customer_id
    order by exported_at_utc_minus_5 desc
) = 1

{% endsnapshot %}