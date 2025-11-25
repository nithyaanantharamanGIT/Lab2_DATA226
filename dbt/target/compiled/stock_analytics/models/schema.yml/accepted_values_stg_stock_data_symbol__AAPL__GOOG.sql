
    
    

with all_values as (

    select
        symbol as value_field,
        count(*) as n_records

    from USER_DB_JACKAL.ANALYTICS_staging.stg_stock_data
    group by symbol

)

select *
from all_values
where value_field not in (
    'AAPL','GOOG'
)


