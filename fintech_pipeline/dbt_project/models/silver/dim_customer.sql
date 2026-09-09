{{
  config(
    materialized='table',
    schema='silver'
  )
}}

select distinct
    customer_id,
    customer_name,
    age,
    country,
    kyc_status,
    email,
    is_valid_age,
    had_schema_drift
from {{ ref('bronze_customers') }}