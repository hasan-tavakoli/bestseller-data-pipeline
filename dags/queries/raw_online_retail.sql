CREATE TABLE IF NOT EXISTS BESTSELLER.RAW.raw_online_retail (
    InvoiceNo STRING,          
    StockCode STRING, 
    Description STRING,
    Quantity INTEGER,
    InvoiceDate TIMESTAMP,
    UnitPrice FLOAT,
    CustomerID STRING,
    Country STRING
);