
    
    

with all_values as (

    select
        combined_signal as value_field,
        count(*) as n_records

    from USER_DB_JACKAL.ANALYTICS_marts.stock_analytics_mart
    group by combined_signal

)

select *
from all_values
where value_field not in (
    'STRONG_BUY','BUY','HOLD','SELL','STRONG_SELL','NEUTRAL'
)


