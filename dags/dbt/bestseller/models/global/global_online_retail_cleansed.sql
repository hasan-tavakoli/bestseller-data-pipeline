{{
  config(
    materialized = 'table'
    )
}} 

WITH src_online_retail AS (
    SELECT
        *
    FROM
        {{ ref('src_online_retail') }} where row_num=1
)

SELECT
    transaction_id,
    product_id,
    product_name,
    quantity,
    transaction_date,
    unit_price,
    customer_id,
    customer_country,
    CASE
        WHEN product_id IS NULL OR product_id = '' THEN 'service'
        WHEN LENGTH(product_id) != 5 OR  product_id REGEXP '^[A-Za-z]+$' THEN 'service'
        ELSE 'product'
    END AS is_service
    ,CASE 
           WHEN LEFT(transaction_id, 1) = 'C' THEN 'cancelled'
           ELSE 'normal'
       END AS transaction_status
       ,
      CASE
        WHEN transaction_status = 'normal' AND quantity > 0 AND unit_price = 0 THEN 'free_or_promotion'
        WHEN transaction_status = 'normal' AND quantity < 0 AND unit_price = 0 THEN 'return_free'
        WHEN transaction_status = 'normal' AND quantity < 0 AND unit_price > 0 THEN 'return_paid'
        WHEN transaction_status = 'cancelled' AND quantity < 0 THEN 'cancelled_return'
        ELSE 'sale'
    END AS transaction_type
FROM
    src_online_retail 
