{{ log_message("Starting to extract data from raw_online_retail source.", level='info') }}

WITH raw_online_retail AS (
    SELECT
        *
    FROM
        {{ source('bestseller', 'online_retail') }}
)
{{ log_message("Data extracted. Counting rows.", level='info') }}
SELECT
    InvoiceNo AS transaction_id,
    CASE
        WHEN StockCode REGEXP '^[0-9]{5}[A-Za-z]+$' THEN REGEXP_REPLACE(SUBSTRING(StockCode, 1, 5), '[^0-9]', '')
        ELSE StockCode
    END AS product_id,
    Description AS product_name,
    Quantity AS quantity,
    InvoiceDate AS transaction_date,
    UnitPrice AS unit_price,
       CASE
        WHEN CustomerID IS NULL THEN 'unknown' 
        WHEN CustomerID LIKE '%.0' THEN SUBSTRING(CustomerID, 1, LENGTH(CustomerID) - 2) 
        ELSE CustomerID  
    END AS customer_id,
    CASE
        WHEN product_id IS NULL OR product_id = '' THEN 'service'
        WHEN LENGTH(product_id) != 5 OR product_id REGEXP '^[A-Za-z]+$' THEN 'service'
        ELSE 'product'
    END AS is_service,
    Country AS customer_country,
    ROW_NUMBER() OVER (
            PARTITION BY InvoiceNo, StockCode, InvoiceDate, Description, Quantity, UnitPrice, CustomerID, Country
            ORDER BY InvoiceNo
        ) AS row_num
FROM
    raw_online_retail


{{ log_message("Transformation src_online_retail completed.", level='info') }}