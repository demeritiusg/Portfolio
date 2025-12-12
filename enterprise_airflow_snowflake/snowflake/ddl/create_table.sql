CREATE OR REPLACE TABLE raw.sales (
    sale_id INT AUTOINCREMENT,
    customer_id INT,
    region STRING,
    sales_amount FLOAT,
    sale_timestamp TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE analytics.sales_summary (
    region STRING,
    total_sales FLOAT,
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
