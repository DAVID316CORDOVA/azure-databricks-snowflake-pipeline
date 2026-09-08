






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and age >= 0 and age <= 120
)
 as expression


    from FINTECH_ANALYTICS.dev_silver.dim_customer
    

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







