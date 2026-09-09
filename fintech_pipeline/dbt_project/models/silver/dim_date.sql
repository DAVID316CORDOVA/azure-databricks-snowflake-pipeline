{{
  config(
    materialized='table',
    schema='silver'
  )
}}

select distinct
    registration_date::date as date_id,
    registration_date::date as full_date,
    date_part('year', registration_date::date) as year,
    date_part('month', registration_date::date) as month,
    date_part('day', registration_date::date) as day,
    date_part('dayofweek', registration_date::date) as day_of_week,
    monthname(registration_date::date) as month_name
from {{ ref('bronze_customers') }}
where registration_date is not null
