{{
  config(
    materialized='view',
    schema='bronze'
  )
}}

select
    customer_id,
    customer_name,
    age,
    country,
    email,
    registration_date,
    account_type,
    account_balance,
    kyc_status,
    metadata:device::string as device,
    metadata:preferences:language::string as language_preference,
    metadata:preferences:notifications::boolean as notifications_enabled,
    metadata:last_login_ip::string as last_login_ip,
    metadata:risk_score::double as risk_score,
    had_schema_drift,
    is_valid_age,
    is_valid_risk_score,
    exported_at_utc_minus_5
from {{ source('raw', 'customers_raw') }}
qualify row_number() over (
    partition by customer_id
    order by exported_at_utc_minus_5 desc
) = 1