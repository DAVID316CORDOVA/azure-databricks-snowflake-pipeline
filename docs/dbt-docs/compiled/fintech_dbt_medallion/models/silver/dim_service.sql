

select distinct
    md5(cast(coalesce(cast(device as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(language_preference as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as device_id,
    device as device_type,
    language_preference
from FINTECH_ANALYTICS.dev_bronze.bronze_customers
where device is not null