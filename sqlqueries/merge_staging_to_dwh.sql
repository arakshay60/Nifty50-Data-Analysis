MERGE INTO nsestockanalysis.stock_dataset.fact_stock_data AS target
USING (
    SELECT 
        symbol, 
        CAST(date AS DATE) AS date,  
        previousClose, 
        open, 
        close, 
        buyQty, 
        sellQty, 
        trade_volume
    FROM nsestockanalysis.stock_dataset.stock_data_staging
) AS source
ON target.symbol = source.symbol AND target.date = source.date
WHEN MATCHED THEN
  UPDATE SET
    target.previousClose = source.previousClose,
    target.open = source.open,
    target.close = source.close,
    target.buyQty = source.buyQty,
    target.sellQty = source.sellQty,
    target.trade_volume = source.trade_volume
WHEN NOT MATCHED THEN
  INSERT (symbol, date, previousClose, open, close, buyQty, sellQty, trade_volume)
  VALUES (source.symbol, source.date, source.previousClose, source.open, source.close, source.buyQty, source.sellQty, source.trade_volume);

MERGE INTO nsestockanalysis.stock_dataset.dim_date AS target
USING (
    SELECT DISTINCT 
        CAST(date AS DATE) AS date,  
        month_name,
        day,
        month,
        year
    FROM nsestockanalysis.stock_dataset.date_staging
) AS source
ON target.date = source.date
WHEN MATCHED THEN
  UPDATE SET
    target.month_name = source.month_name,
    target.day = source.day,
    target.month = source.month,
    target.year = source.year
WHEN NOT MATCHED THEN
  INSERT (date, month_name, day, month, year)
  VALUES (source.date, source.month_name, source.day, source.month, source.year);


MERGE INTO nsestockanalysis.stock_dataset.dim_company AS target
USING nsestockanalysis.stock_dataset.company_staging AS source
ON target.symbol = source.symbol
WHEN MATCHED THEN
  UPDATE SET
    target.companyName = source.companyName,
    target.industry = source.industry
WHEN NOT MATCHED THEN
  INSERT (symbol, companyName, industry)
  VALUES (source.symbol, source.companyName, source.industry);
