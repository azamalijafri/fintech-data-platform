SELECT
    step,
    type,
    amount,
    nameOrig,
    oldbalanceOrg,
    newbalanceOrig,
    nameDest,
    oldbalanceDest,
    newbalanceDest,
    isFraud,
    isFlaggedFraud,

    CASE
        WHEN isFraud = 1 THEN 'FRAUD'
        ELSE 'LEGITIMATE'
    END AS fraud_status,

    CASE
        WHEN amount >= 100000 THEN 'HIGH'
        WHEN amount >= 10000 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS transaction_amount_category

FROM {{ ref('stg_transactions') }}
