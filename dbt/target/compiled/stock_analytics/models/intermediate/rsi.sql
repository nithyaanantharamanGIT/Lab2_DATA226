

WITH stock_data AS (
    SELECT
        symbol,
        date,
        close
    FROM USER_DB_JACKAL.ANALYTICS_staging.stg_stock_data
    WHERE has_null_prices = FALSE
        AND has_invalid_prices = FALSE
),

price_changes AS (
    SELECT
        symbol,
        date,
        close,
        close - LAG(close, 1) OVER (PARTITION BY symbol ORDER BY date) AS price_change
    FROM stock_data
),

gains_losses AS (
    SELECT
        symbol,
        date,
        close,
        price_change,
        CASE WHEN price_change > 0 THEN price_change ELSE 0 END AS gain,
        CASE WHEN price_change < 0 THEN ABS(price_change) ELSE 0 END AS loss
    FROM price_changes
    WHERE price_change IS NOT NULL
),

average_gains_losses AS (
    SELECT
        symbol,
        date,
        close,
        price_change,
        gain,
        loss,
        -- 14-period average gain
        AVG(gain) OVER (
            PARTITION BY symbol 
            ORDER BY date 
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS avg_gain_14,
        -- 14-period average loss
        AVG(loss) OVER (
            PARTITION BY symbol 
            ORDER BY date 
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS avg_loss_14,
        -- Count periods for validation
        COUNT(*) OVER (
            PARTITION BY symbol 
            ORDER BY date 
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS period_count
    FROM gains_losses
),

rsi_calculation AS (
    SELECT
        symbol,
        date,
        close,
        price_change,
        avg_gain_14,
        avg_loss_14,
        period_count,
        -- Calculate RS (Relative Strength)
        CASE 
            WHEN avg_loss_14 = 0 THEN NULL
            ELSE avg_gain_14 / NULLIF(avg_loss_14, 0)
        END AS rs,
        -- Calculate RSI
        CASE 
            WHEN avg_loss_14 = 0 THEN 100
            WHEN avg_gain_14 = 0 THEN 0
            ELSE 100 - (100 / (1 + (avg_gain_14 / NULLIF(avg_loss_14, 0))))
        END AS rsi_14
    FROM average_gains_losses
    WHERE period_count >= 14  -- Ensure we have enough periods
)

SELECT
    symbol,
    date,
    close,
    ROUND(price_change, 2) AS price_change,
    ROUND(rsi_14, 2) AS rsi_14,
    -- RSI interpretation signals
    CASE 
        WHEN rsi_14 >= 70 THEN 'OVERBOUGHT'
        WHEN rsi_14 <= 30 THEN 'OVERSOLD'
        WHEN rsi_14 > 50 THEN 'BULLISH'
        WHEN rsi_14 < 50 THEN 'BEARISH'
        ELSE 'NEUTRAL'
    END AS rsi_signal,
    -- RSI momentum
    CASE 
        WHEN rsi_14 > LAG(rsi_14, 1) OVER (PARTITION BY symbol ORDER BY date) 
        THEN 'INCREASING'
        WHEN rsi_14 < LAG(rsi_14, 1) OVER (PARTITION BY symbol ORDER BY date) 
        THEN 'DECREASING'
        ELSE 'FLAT'
    END AS rsi_momentum
FROM rsi_calculation
ORDER BY symbol, date