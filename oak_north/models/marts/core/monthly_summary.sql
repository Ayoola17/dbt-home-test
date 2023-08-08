-- 
-- MONTHLY_TOTAL CTE: 
-- This section computes the total transaction amount for each month over the past 12 months.
-- We extract the year and month from the transaction_date and aggregate the amounts.
-- Filtering only transactions from the last 12 months.
--
WITH monthly_total AS (
    SELECT
        strftime('%Y-%m', transaction_date) AS transaction_month,
        SUM(amount) AS total_amount
    FROM {{ ref('stg_transactions') }}
    WHERE transaction_date >= date('now', '-12 month')
    GROUP BY 1
),
-- 
-- MONTHLY_TOTAL_WITH_CHANGE CTE:
-- Here, for each month's total, we compute the percentage change in transaction totals from one month to the next.
-- We use the LAG window function to retrieve the previous month's total amount and compute the percentage change.
--
monthly_total_with_change AS (
    SELECT
        transaction_month,
        total_amount,
        ((total_amount - LAG(total_amount) OVER (ORDER BY transaction_month)) / LAG(total_amount) OVER (ORDER BY transaction_month)) * 100 AS percent_change
    FROM monthly_total
),
-- 
-- TOP_CUSTOMERS CTE:
-- This segment determines the total amount spent by each customer over the past 12 months, month-by-month.
-- We join the transactions and customers tables and group by month, customer ID, and customer name.
-- This way, for each month, we get each customer's spending amount.
--
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
-- 
-- TOP_CUSTOMER_PER_MONTH CTE:
-- For each month, this section ranks customers based on their spending.
-- Using the RANK() window function, we order customers by their spending amount in descending order.
-- This allows us to later filter for the top spender of each month.
--
top_customer_per_month AS (
    SELECT
        transaction_month,
        customer_id,
        name,
        customer_total,
        RANK() OVER (PARTITION BY transaction_month ORDER BY customer_total DESC) as rank
    FROM top_customers
)
-- 
-- FINAL QUERY:
-- We join our computed monthly totals (with percentage changes) with our top customers for each month.
-- We ensure that we're only taking the top customer for each month (i.e., rank = 1).
-- The result will give us a comprehensive view of monthly transaction totals, their change in value, and the highest spender for each month.
--
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
