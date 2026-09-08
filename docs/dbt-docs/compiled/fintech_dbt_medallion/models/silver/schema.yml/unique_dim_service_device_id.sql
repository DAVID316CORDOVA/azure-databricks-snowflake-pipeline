
    
    

select
    device_id as unique_field,
    count(*) as n_records

from FINTECH_ANALYTICS.dev_silver.dim_service
where device_id is not null
group by device_id
having count(*) > 1


