select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select date
from USER_DB_JACKAL.ANALYTICS_marts.stock_analytics_mart
where date is null



      
    ) dbt_internal_test