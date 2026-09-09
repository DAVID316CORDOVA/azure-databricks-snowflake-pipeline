{{
  config(
    materialized='table',
    schema='gold'
  )
}}

select
    d.country,
    dt.year,
    dt.month,
    count(distinct f.customer_id) as total_customers,
    avg(f.account_balance) as avg_balance,
    sum(f.account_balance) as total_balance,
    avg(f.risk_score) as avg_risk_score,
    sum(case when d.kyc_status = 'pending' then 1 else 0 end) as pending_kyc_count,
    sum(case when d.kyc_status = 'rejected' then 1 else 0 end) as rejected_kyc_count
from {{ ref('fact_customer_activity') }} f
join {{ ref('dim_customer') }} d on f.customer_id = d.customer_id
join {{ ref('dim_date') }} dt on f.registration_date::date = dt.date_id
group by d.country, dt.year, dt.month
