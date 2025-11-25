





with validation_errors as (

    select
        symbol, date
    from USER_DB_JACKAL.ANALYTICS_intermediate.moving_averages
    group by symbol, date
    having count(*) > 1

)

select *
from validation_errors


