WITH base AS (
    SELECT
        c.customer_id,
        c.name,
        c.date_of_birth,
        c.joined_date,
        t.transaction_id,
        t.transaction_date,
        t.amount,
        strftime('%m', t.transaction_date) AS transaction_month,
        LAG(t.transaction_date) OVER (PARTITION BY c.customer_id ORDER BY t.transaction_date) AS previous_transaction_date
    FROM {{ ref('stg_customers') }} c
    LEFT JOIN {{ ref('stg_transactions') }} t
    ON c.customer_id = t.customer_id
    WHERE c.customer_id IS NOT NULL
    AND c.name IS NOT NULL
    AND c.date_of_birth IS NOT NULL
    AND c.joined_date IS NOT NULL
    AND t.transaction_id IS NOT NULL
    AND t.amount IS NOT NULL
    AND CAST(strftime('%Y', c.date_of_birth) AS INT) < 2100
    AND CAST(strftime('%Y', c.joined_date) AS INT) < 2100  
    AND CAST(strftime('%Y', t.transaction_date) AS INT) <= 2100 
),

intervals AS (
    SELECT
        customer_id,
        transaction_id,
        transaction_date,
        transaction_month,
        amount,
        CASE 
            WHEN previous_transaction_date IS NOT NULL THEN julianday(transaction_date) - julianday(previous_transaction_date)
            ELSE NULL 
        END AS spending_interval
    FROM base
),

aggregates AS (
    SELECT
        customer_id,
        COUNT(DISTINCT transaction_id) AS total_transactions,
        AVG(amount) AS avg_monthly_spending,
        AVG(spending_interval) AS avg_spending_interval
    FROM intervals
    GROUP BY customer_id
)

SELECT
    b.customer_id,
    b.name,
    b.date_of_birth,
    b.joined_date,
    a.total_transactions,
    a.avg_monthly_spending,
    a.avg_spending_interval
FROM base b
LEFT JOIN aggregates a
ON b.customer_id = a.customer_id
GROUP BY 1, 2, 3, 4

