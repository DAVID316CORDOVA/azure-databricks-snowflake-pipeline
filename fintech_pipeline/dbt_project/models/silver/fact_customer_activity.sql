{{
  config(
    materialized='incremental',
    unique_key='customer_id',
    schema='silver'
  )
}}

select
    customer_id,
    account_balance,
    risk_score,
    is_valid_risk_score,
    registration_date,
    exported_at_utc_minus_5
from {{ ref('bronze_customers') }}

{% if is_incremental() %}
  where exported_at_utc_minus_5 > (select max(exported_at_utc_minus_5) from {{ this }})
{% endif %}