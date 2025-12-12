MERGE INTO analytics.sales_summary AS tgt
USING (
    SELECT region, SUM(sales_amount) AS total_sales
    FROM raw.sales
    GROUP BY region
) AS src
ON tgt.region = src.region
WHEN MATCHED THEN
    UPDATE SET tgt.total_sales = src.total_sales, tgt.load_timestamp = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (region, total_sales) VALUES (src.region, src.total_sales);
