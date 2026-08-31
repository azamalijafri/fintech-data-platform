SELECT
    type,
    fraud_status,
    transaction_amount_category,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_amount,
    AVG(amount) AS average_amount,
    SUM(CASE WHEN isFraud = 1 THEN amount ELSE 0 END) AS total_fraud_amount
FROM {{ ref('int_transactions') }}
GROUP BY
    type,
    fraud_status,
    transaction_amount_category