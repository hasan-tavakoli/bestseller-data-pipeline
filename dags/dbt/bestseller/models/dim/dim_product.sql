{{
 config(
   materialized = 'incremental',
   on_schema_change='fail'
 )
}}

WITH global_online_retail_cleansed AS (
    SELECT 
        product_id,
        product_name,
    FROM {{ ref('global_online_retail_cleansed') }} 
    WHERE  
    transaction_status='normal' and 
    transaction_type='sale'  and
    is_service='product'
    GROUP BY product_id, product_name
)
SELECT 
    {{ dbt_utils.surrogate_key(['lc.product_id', 'lc.product_name']) }} AS product_unique_id,
    lc.product_id,
    lc.product_name,
FROM global_online_retail_cleansed lc

{% if is_incremental() %}
    LEFT JOIN {{ this }} t
    ON lc.product_id = t.product_id
    WHERE 
        t.product_id IS NULL
        OR 
        lc.product_name != t.product_name  
{% endif %}
