{{ config(
materialized = 'table',
) }}


WITH transactions AS (
    SELECT 
        tr.customer_id,
        tr.transaction_id,
        tr.total_amount,
        tr.transaction_date,
        tr.transaction_date_numeric
    FROM {{ref('fct_transactions')}} tr where customer_id != 'unknown'
),
dim_dates AS (
    SELECT 
        d.datenum,
        d.monthnum,
        d.monthname,
        d.calendar_quarter,
        d.weeknum,
        d.yearquarternum
    FROM {{ ref('dim_date') }} as d
),
customer_transactions AS (
    SELECT 
        t.customer_id,
        t.transaction_id,
        t.total_amount,
        t.transaction_date,
        dd.datenum,
        dd.monthnum,
        dd.monthname,
        dd.calendar_quarter,
        dd.weeknum,
        dd.yearquarternum
    FROM transactions t
    JOIN dim_dates dd 
    ON t.transaction_date_numeric = dd.datenum
)

SELECT 
    ct.customer_id,
    DATEDIFF(day, MAX(ct.transaction_date), CURRENT_DATE) AS recency,
    COUNT(ct.transaction_id) AS frequency,
    SUM(ct.total_amount) AS monetary,
    MAX(ct.monthnum) AS month_number,
    MAX(ct.monthname) AS month_name,
    MAX(ct.calendar_quarter) AS calendar_quarter,
    MAX(ct.weeknum) AS week_number,
    MAX(ct.yearquarternum) AS year_quarter_number

FROM customer_transactions ct
GROUP BY ct.customer_id
ORDER BY recency ASC
