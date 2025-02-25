TRUNCATE TABLE nsestockanalysis.stock_dataset.price_change_table;

INSERT INTO nsestockanalysis.stock_dataset.price_change_table
SELECT
    symbol,
    close,
    ROUND(SAFE_DIVIDE((close - previousClose), previousClose) * 100, 3) AS percent_change_1d,
    ROUND(SAFE_DIVIDE(close - close_1w, close_1w) * 100, 3) AS percent_change_1w,
    ROUND(SAFE_DIVIDE(close - close_1m, close_1m) * 100, 3) AS percent_change_1m
FROM (
    SELECT
        curr.symbol,
        curr.date,
        curr.close,
        curr.previousClose,
        prev_week.close AS close_1w,
        prev_month.close AS close_1m
    FROM nsestockanalysis.stock_dataset.fact_stock_data curr
    LEFT JOIN nsestockanalysis.stock_dataset.fact_stock_data prev_week
        ON curr.symbol = prev_week.symbol
        AND prev_week.date = DATE_SUB(curr.date, INTERVAL 7 DAY)
    LEFT JOIN nsestockanalysis.stock_dataset.fact_stock_data prev_month
        ON curr.symbol = prev_month.symbol
        AND prev_month.date = DATE_SUB(curr.date, INTERVAL 30 DAY)
    WHERE curr.date = (SELECT MAX(date) FROM nsestockanalysis.stock_dataset.fact_stock_data)
);

TRUNCATE TABLE nsestockanalysis.stock_dataset.buy_sell_table;

INSERT INTO nsestockanalysis.stock_dataset.buy_sell_table
SELECT
    symbol,
    ROUND(SAFE_DIVIDE(buyQty, sellQty), 2) AS buy_sell_ratio_1d,
    ROUND(SAFE_DIVIDE(buyQty_1w, sellQty_1w), 2) AS buy_sell_ratio_1w,
    ROUND(SAFE_DIVIDE(buyQty_1m, sellQty_1m), 2) AS buy_sell_ratio_1m
FROM (
    SELECT
        curr.symbol,
        curr.date,
        curr.buyQty,
        curr.sellQty,
        prev_week.buyQty AS buyQty_1w,
        prev_week.sellQty AS sellQty_1w,
        prev_month.buyQty AS buyQty_1m,
        prev_month.sellQty AS sellQty_1m
    FROM nsestockanalysis.stock_dataset.fact_stock_data curr
    LEFT JOIN nsestockanalysis.stock_dataset.fact_stock_data prev_week
        ON curr.symbol = prev_week.symbol
        AND prev_week.date = DATE_SUB(curr.date, INTERVAL 7 DAY)
    LEFT JOIN nsestockanalysis.stock_dataset.fact_stock_data prev_month
        ON curr.symbol = prev_month.symbol
        AND prev_month.date = DATE_SUB(curr.date, INTERVAL 30 DAY)
    WHERE curr.date = (SELECT MAX(date) FROM nsestockanalysis.stock_dataset.fact_stock_data)
);
