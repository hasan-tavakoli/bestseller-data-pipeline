{{
 config(
   materialized = 'incremental',
   on_schema_change='fail'
 )
}}
{{ log_message('Starting incremental model processing for global_online_retail_cleansed.', level='info') }}
WITH global_online_retail_cleansed AS (
    SELECT 
        transaction_id,
        customer_id,
        product_id,
        product_name,
        customer_country,
        transaction_date,
        TO_CHAR(transaction_date, 'YYYYMMDD') AS transaction_date_numeric,
        TO_CHAR(EXTRACT(HOUR FROM transaction_date), '00') || ':' || TO_CHAR(EXTRACT(MINUTE FROM transaction_date), '00') AS transaction_hour_minute, 
        quantity,
        unit_price,
        quantity * unit_price AS total_amount
    FROM {{ ref('global_online_retail_cleansed') }} 
    WHERE  
        transaction_status='normal' 
        AND transaction_type='sale'  
        AND is_service='product'
),
product_ids AS (
    SELECT 
        product_unique_id,
        product_id,
        product_name,
    FROM {{ ref('dim_product') }}
),
customer_ids AS (
    SELECT 
        customer_unique_id,
        customer_id,
        customer_country,
    FROM {{ ref('dim_customer') }}
)
{{ log_message('Joining global_online_retail_cleansed with product and customer tables.', level='info') }}
SELECT  
     gr.transaction_id,
     pi.product_unique_id,
     ci.customer_unique_id,
     gr.product_id,
     gr.customer_id,
     gr.product_name,
     gr.customer_country,
     gr.transaction_date,
     gr.transaction_date_numeric,
     gr.transaction_hour_minute, 
     gr.quantity,
     gr.unit_price,
     total_amount
     
FROM global_online_retail_cleansed gr
JOIN product_ids pi
    ON gr.product_id = pi.product_id
    AND gr.product_name = pi.product_name
JOIN customer_ids ci
    ON gr.customer_id = ci.customer_id
    AND gr.customer_country = ci.customer_country
{% if is_incremental() %}
WHERE gr.transaction_date > (select max(transaction_date) from {{ this }})
{% endif %}

{{ log_message("Product transformation fct_transactions successfully.", level='info') }}