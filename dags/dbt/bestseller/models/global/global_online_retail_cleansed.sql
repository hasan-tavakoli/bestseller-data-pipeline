{{
  config(
    materialized = 'table'
  )
}} 

{{ log_message("Starting extraction from src_online_retail source.", level='info') }}

WITH src_online_retail AS (
    SELECT
        *
    FROM
        {{ ref('src_online_retail') }} 
    WHERE row_num = 1
)

{{ log_message("Data extracted from src_online_retail. Transforming data.", level='info') }}

SELECT
    CASE
        WHEN transaction_id REGEXP '^[A-Za-z]\\d{6}$' AND NOT LEFT(transaction_id, 1) = 'C'
        THEN REGEXP_REPLACE(transaction_id, '^[A-Za-z]', '')
        ELSE transaction_id
    END AS transaction_id,
    product_id,
    product_name,
    quantity,
    transaction_date,
    unit_price,
    customer_id,
    customer_country,   
    CASE 
        WHEN LEFT(transaction_id, 1) = 'C' THEN 'cancelled'
        ELSE 'normal'
    END AS transaction_status,

    CASE
        WHEN transaction_status = 'normal' AND quantity > 0 AND unit_price = 0 THEN 'free_or_promotion'
        WHEN transaction_status = 'normal' AND quantity < 0 AND unit_price = 0 THEN 'return_free'
        WHEN transaction_status = 'normal' AND quantity < 0 AND unit_price > 0 THEN 'return_paid'
        WHEN transaction_status = 'cancelled' AND quantity < 0 THEN 'cancelled_return'
        ELSE 'sale'
    END AS transaction_type
FROM
    src_online_retail where is_service='product'

{{ log_message("Transformation completed. Data processed:global_online_retail_cleansed successfully.", level='info') }}
