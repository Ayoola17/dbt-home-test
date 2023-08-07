WITH monthly_total AS (
    SELECT
        strftime('%Y-%m', transaction_date) AS transaction_month,
        SUM(amount) AS total_amount
    FROM {{ ref('stg_transactions') }}
    WHERE transaction_date >= date('now', '-12 month')
    GROUP BY 1
),
monthly_total_with_change AS (
    SELECT
        transaction_month,
        total_amount,
        ((total_amount - LAG(total_amount) OVER (ORDER BY transaction_month)) / LAG(total_amount) OVER (ORDER BY transaction_month)) * 100 AS percent_change
    FROM monthly_total
),
top_customers AS (
    SELECT
        strftime('%Y-%m', t.transaction_date) AS transaction_month,
        t.customer_id,
        c.name,
        SUM(t.amount) AS customer_total
    FROM {{ ref('stg_transactions') }} t
    JOIN {{ ref('stg_customers') }} c ON t.customer_id = c.customer_id
    WHERE t.transaction_date >= date('now', '-12 month')
    GROUP BY 1, 2, 3
),
top_customer_per_month AS (
    SELECT
        transaction_month,
        customer_id,
        name,
        customer_total,
        RANK() OVER (PARTITION BY transaction_month ORDER BY customer_total DESC) as rank
    FROM top_customers
)
SELECT
    m.transaction_month,
    t.customer_id,
    t.name as customer_name,
    m.total_amount,
    m.percent_change,
    t.customer_total
FROM monthly_total_with_change m
JOIN top_customer_per_month t ON m.transaction_month = t.transaction_month
WHERE t.rank = 1
