TRUNCATE TABLE `nsestockanalysis.stock_dataset.price_change_table`;
TRUNCATE TABLE `nsestockanalysis.stock_dataset.price_change_table`;

INSERT INTO `nsestockanalysis.stock_dataset.price_change_table`
SELECT 
    symbol,
    close,
    ROUND(SAFE_DIVIDE((close - previousClose), previousClose) * 100, 3) AS percent_change_1d,
    ROUND(SAFE_DIVIDE(close - close_1w, close_1w) * 100, 3) AS percent_change_1w,
    ROUND(SAFE_DIVIDE(close - close_1m, close_1m) * 100, 3) AS percent_change_1m
FROM (
    SELECT 
        symbol, 
        date,
        close,
        previousClose,
        LAG(close, 7) OVER (PARTITION BY symbol ORDER BY date) AS close_1w,
        LAG(close, 30) OVER (PARTITION BY symbol ORDER BY date) AS close_1m
    FROM `nsestockanalysis.stock_dataset.fact_stock_data`
)
WHERE date = (SELECT MAX(date) FROM `nsestockanalysis.stock_dataset.fact_stock_data`);

INSERT INTO `nsestockanalysis.stock_dataset.buy_sell_table`
SELECT 
    symbol,
    ROUND(SAFE_DIVIDE(buyQty, sellQty), 2) AS buy_sell_ratio_1d,
    ROUND(SAFE_DIVIDE(buyQty_1w, sellQty_1w), 2) AS buy_sell_ratio_1w,
    ROUND(SAFE_DIVIDE(buyQty_1m, sellQty_1m), 2) AS buy_sell_ratio_1m
FROM (
    SELECT 
        symbol, 
        date,
        buyQty,
        sellQty,
        LAG(buyQty, 1) OVER (PARTITION BY symbol ORDER BY date) AS buyQty_1d,
        LAG(sellQty, 1) OVER (PARTITION BY symbol ORDER BY date) AS sellQty_1d,
        LAG(buyQty, 7) OVER (PARTITION BY symbol ORDER BY date) AS buyQty_1w,
        LAG(sellQty, 7) OVER (PARTITION BY symbol ORDER BY date) AS sellQty_1w,
        LAG(buyQty, 30) OVER (PARTITION BY symbol ORDER BY date) AS buyQty_1m,
        LAG(sellQty, 30) OVER (PARTITION BY symbol ORDER BY date) AS sellQty_1m
    FROM `nsestockanalysis.stock_dataset.fact_stock_data`
)
WHERE date = (SELECT MAX(date) FROM `nsestockanalysis.stock_dataset.fact_stock_data`);

