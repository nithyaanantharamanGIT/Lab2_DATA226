
  
    

        create or replace transient table USER_DB_JACKAL.ANALYTICS_marts.stock_analytics_mart
         as
        (

WITH moving_averages AS (
    SELECT * FROM USER_DB_JACKAL.ANALYTICS_intermediate.moving_averages
),

rsi AS (
    SELECT * FROM USER_DB_JACKAL.ANALYTICS_intermediate.rsi
),

combined_analytics AS (
    SELECT
        ma.symbol,
        ma.date,
        ma.close,
        -- Moving Averages
        ma.sma_7,
        ma.sma_14,
        ma.sma_30,
        ma.sma_50,
        ma.signal_7_14,
        ma.signal_14_30,
        ma.pct_from_sma_50,
        -- RSI Indicators
        rsi.rsi_14,
        rsi.rsi_signal,
        rsi.rsi_momentum,
        rsi.price_change,
        -- Combined trading signal
        CASE
            WHEN rsi.rsi_14 <= 30 AND ma.signal_7_14 = 'BULLISH_SHORT' THEN 'STRONG_BUY'
            WHEN rsi.rsi_14 >= 70 AND ma.signal_7_14 = 'BEARISH_SHORT' THEN 'STRONG_SELL'
            WHEN rsi.rsi_14 < 40 AND ma.signal_14_30 = 'BULLISH_MED' THEN 'BUY'
            WHEN rsi.rsi_14 > 60 AND ma.signal_14_30 = 'BEARISH_MED' THEN 'SELL'
            WHEN rsi.rsi_14 BETWEEN 40 AND 60 THEN 'HOLD'
            ELSE 'NEUTRAL'
        END AS combined_signal,
        -- Volatility indicator (price % change)
        ROUND((rsi.price_change / NULLIF(ma.close - rsi.price_change, 0)) * 100, 2) AS daily_return_pct,
        -- Trend strength
        CASE
            WHEN ma.close > ma.sma_7 
                AND ma.sma_7 > ma.sma_14 
                AND ma.sma_14 > ma.sma_30 
            THEN 'STRONG_UPTREND'
            WHEN ma.close < ma.sma_7 
                AND ma.sma_7 < ma.sma_14 
                AND ma.sma_14 < ma.sma_30 
            THEN 'STRONG_DOWNTREND'
            WHEN ma.close > ma.sma_30 THEN 'UPTREND'
            WHEN ma.close < ma.sma_30 THEN 'DOWNTREND'
            ELSE 'SIDEWAYS'
        END AS trend_strength
    FROM moving_averages ma
    INNER JOIN rsi 
        ON ma.symbol = rsi.symbol 
        AND ma.date = rsi.date
)

SELECT
    symbol,
    date,
    close,
    sma_7,
    sma_14,
    sma_30,
    sma_50,
    rsi_14,
    signal_7_14,
    signal_14_30,
    rsi_signal,
    rsi_momentum,
    combined_signal,
    trend_strength,
    pct_from_sma_50,
    daily_return_pct,
    price_change
FROM combined_analytics
ORDER BY symbol, date DESC
        );
      
  