
  create or replace   view USER_DB_JACKAL.ANALYTICS_staging.stg_stock_data
  
   as (
    

WITH source_data AS (
    SELECT
        symbol,
        date,
        open,
        high,
        low,
        close,
        volume
    FROM USER_DB_JACKAL.RAW.stock_data_lab
    WHERE date IS NOT NULL
        AND symbol IS NOT NULL
)

SELECT
    symbol,
    date,
    open,
    high,
    low,
    close,
    volume,
    -- Add data quality flags
    CASE 
        WHEN open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL 
        THEN TRUE 
        ELSE FALSE 
    END AS has_null_prices,
    -- Validate price relationships
    CASE 
        WHEN high < low 
            OR high < open 
            OR high < close 
            OR low > open 
            OR low > close 
        THEN TRUE 
        ELSE FALSE 
    END AS has_invalid_prices
FROM source_data
  );

