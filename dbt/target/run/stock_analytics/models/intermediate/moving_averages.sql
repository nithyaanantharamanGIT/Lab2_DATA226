
  
    

        create or replace transient table USER_DB_JACKAL.ANALYTICS_intermediate.moving_averages
         as
        (

WITH stock_data AS (
    SELECT
        symbol,
        date,
        close
    FROM USER_DB_JACKAL.ANALYTICS_staging.stg_stock_data
    WHERE has_null_prices = FALSE
        AND has_invalid_prices = FALSE
),

moving_averages AS (
    SELECT
        symbol,
        date,
        close,
        -- 7-day Simple Moving Average
        AVG(close) OVER (
            PARTITION BY symbol 
            ORDER BY date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS sma_7,
        -- 14-day Simple Moving Average
        AVG(close) OVER (
            PARTITION BY symbol 
            ORDER BY date 
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS sma_14,
        -- 30-day Simple Moving Average
        AVG(close) OVER (
            PARTITION BY symbol 
            ORDER BY date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS sma_30,
        -- 50-day Simple Moving Average
        AVG(close) OVER (
            PARTITION BY symbol 
            ORDER BY date 
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ) AS sma_50,
        -- Count of days for MA calculation validation
        COUNT(*) OVER (
            PARTITION BY symbol 
            ORDER BY date 
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ) AS days_count
    FROM stock_data
)

SELECT
    symbol,
    date,
    close,
    ROUND(sma_7, 2) AS sma_7,
    ROUND(sma_14, 2) AS sma_14,
    ROUND(sma_30, 2) AS sma_30,
    ROUND(sma_50, 2) AS sma_50,
    -- Calculate crossover signals
    CASE 
        WHEN sma_7 > sma_14 THEN 'BULLISH_SHORT'
        WHEN sma_7 < sma_14 THEN 'BEARISH_SHORT'
        ELSE 'NEUTRAL'
    END AS signal_7_14,
    CASE 
        WHEN sma_14 > sma_30 THEN 'BULLISH_MED'
        WHEN sma_14 < sma_30 THEN 'BEARISH_MED'
        ELSE 'NEUTRAL'
    END AS signal_14_30,
    -- Price vs MA comparison
    ROUND(((close - sma_50) / NULLIF(sma_50, 0)) * 100, 2) AS pct_from_sma_50
FROM moving_averages
WHERE days_count >= 7  -- Ensure we have enough data for at least the shortest MA
        );
      
  