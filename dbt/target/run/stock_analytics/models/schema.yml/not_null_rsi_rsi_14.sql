select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select rsi_14
from USER_DB_JACKAL.ANALYTICS_intermediate.rsi
where rsi_14 is null



      
    ) dbt_internal_test