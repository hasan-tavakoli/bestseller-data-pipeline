{{
 config(
   materialized = 'incremental',
   on_schema_change='fail'
 )
}}

WITH global_online_retail_cleansed AS (
    SELECT 
        customer_id,
        customer_country,
        MIN(transaction_date) AS earliest_transaction_date 
    FROM {{ ref('global_online_retail_cleansed') }} 
    WHERE customer_id != 'unknown' 
        AND transaction_status='normal' 
        AND transaction_type='sale'  
        AND is_service='product'
    GROUP BY customer_id, customer_country
)
SELECT 
    {{ dbt_utils.surrogate_key(['lc.customer_id', 'lc.customer_country']) }} AS customer_unique_id,
    lc.customer_id,  
    lc.customer_country,  
    lc.earliest_transaction_date
FROM global_online_retail_cleansed lc

{% if is_incremental() %}
    LEFT JOIN {{ this }} t
    ON lc.customer_id = t.customer_id
    WHERE 
        t.customer_id IS NULL  
        OR 
        lc.customer_country != t.customer_country  
{% endif %}