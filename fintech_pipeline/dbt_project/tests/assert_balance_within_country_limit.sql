{{ config(severity='warn') }}

select
    f.customer_id,
    f.account_balance,
    d.country,
    c.max_balance_limit
from {{ ref('fact_customer_activity') }} f
join {{ ref('dim_customer') }} d on f.customer_id = d.customer_id
join {{ ref('country_compliance_limits') }} c on d.country = c.country
where f.account_balance > c.max_balance_limit