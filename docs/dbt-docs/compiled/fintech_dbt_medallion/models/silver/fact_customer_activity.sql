

select
    customer_id,
    account_balance,
    risk_score,
    is_valid_risk_score,
    registration_date,
    exported_at_utc_minus_5
from FINTECH_ANALYTICS.dev_bronze.bronze_customers


  where exported_at_utc_minus_5 > (select max(exported_at_utc_minus_5) from FINTECH_ANALYTICS.dev_silver.fact_customer_activity)
