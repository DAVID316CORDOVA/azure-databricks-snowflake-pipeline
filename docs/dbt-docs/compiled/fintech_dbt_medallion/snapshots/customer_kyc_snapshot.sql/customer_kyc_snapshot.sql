



select
    customer_id,
    kyc_status,
    account_balance,
    exported_at_utc_minus_5
from FINTECH_ANALYTICS.RAW.customers_raw
qualify row_number() over (
    partition by customer_id
    order by exported_at_utc_minus_5 desc
) = 1
