




    with grouped_expression as (
    select
        
        
    
  account_balance is not null as expression


    from FINTECH_ANALYTICS.dev_silver.fact_customer_activity
    

),
validation_errors as (

    select
        *
    from
        grouped_expression
    where
        not(expression = true)

)

select *
from validation_errors



