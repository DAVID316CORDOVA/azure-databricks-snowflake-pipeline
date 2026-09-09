






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and risk_score >= -50 and risk_score <= 200
)
 as expression


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







