{{
  config(
    materialized='table',
    schema='silver'
  )
}}

select distinct
    {{ dbt_utils.generate_surrogate_key(['device', 'language_preference']) }} as device_id,
    device as device_type,
    language_preference
from {{ ref('bronze_customers') }}
where device is not null