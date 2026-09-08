

select
    f.customer_id,
    f.account_balance,
    d.country,
    c.max_balance_limit
from FINTECH_ANALYTICS.dev_silver.fact_customer_activity f
join FINTECH_ANALYTICS.dev_silver.dim_customer d on f.customer_id = d.customer_id
join FINTECH_ANALYTICS.dev.country_compliance_limits c on d.country = c.country
where f.account_balance > c.max_balance_limit