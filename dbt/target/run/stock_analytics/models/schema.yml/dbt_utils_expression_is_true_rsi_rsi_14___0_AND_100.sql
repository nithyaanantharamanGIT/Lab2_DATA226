select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      



select
    1
from USER_DB_JACKAL.ANALYTICS_intermediate.rsi

where not(rsi_14 >= 0 AND <= 100)


      
    ) dbt_internal_test