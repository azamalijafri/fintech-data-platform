# Snowflake ↔ Amazon S3 Storage Integration — Setup Runbook

Personal reference: exact order of operations for wiring Snowflake to read S3 via an IAM role. This is the sequence that actually works — the tricky part is that the IAM role and the Snowflake storage integration each need a piece of information _from the other one_, so they can't both be finished in one shot. You go back and forth.

---

## Order of operations (high level)

```
1. Create IAM role with a placeholder trust policy
2. Attach the S3 permissions policy to the role
3. Create the Snowflake storage integration, pointing at the role ARN
4. DESC the integration → get Snowflake's IAM user ARN + external ID
5. Go back to AWS → update the role's trust policy with those two values
6. Create the file format
7. Create the external stage
8. Validate: LIST the stage
9. Create the target table
10. COPY INTO
```

Steps 1→3 need the role to exist before the integration can reference it. Steps 4→5 need the integration to exist before the role's trust policy can be finalized. That's why the role is created twice, effectively — once with a placeholder trust policy, once patched.

---

## Step 1 — Create the IAM role (placeholder trust policy)

You don't know Snowflake's principal/external ID yet, so start with a trust policy that will be replaced in Step 5. Some people use a temporary trust policy that trusts their own account so the role is at least creatable; it gets overwritten in Step 5 either way.

```bash
aws iam create-role \
  --role-name <ROLE_NAME> \
  --assume-role-policy-document file://trust-policy-placeholder.json
```

`trust-policy-placeholder.json` — anything valid; it gets replaced in Step 5:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "AWS": "arn:aws:iam::<AWS_ACCOUNT_ID>:root" },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

---

## Step 2 — Attach the S3 permissions policy

```bash
aws iam put-role-policy \
  --role-name <ROLE_NAME> \
  --policy-name <POLICY_NAME> \
  --policy-document file://permissions-policy.json
```

`permissions-policy.json`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::<PROJECT_BUCKET>",
        "arn:aws:s3:::<PROJECT_BUCKET>/raw/transactions/*"
      ]
    }
  ]
}
```

Note the split: the bucket ARN (no path) for `ListBucket`/`GetBucketLocation`, and the bucket ARN **with** the path + wildcard for `GetObject`/`GetObjectVersion`. Mixing these up is a common cause of "access denied" even when the role looks right.

Grab the role ARN now — you need it in Step 3:

```bash
aws iam get-role --role-name <ROLE_NAME> --query 'Role.Arn' --output text
```

---

## Step 3 — Create the Snowflake storage integration

```sql
CREATE STORAGE INTEGRATION FINTECH_S3_STORAGE
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = '<ROLE_ARN_FROM_STEP_2>'
  STORAGE_ALLOWED_LOCATIONS = ('s3://<PROJECT_BUCKET>/raw/transactions/');
```

`STORAGE_ALLOWED_LOCATIONS` isn't strictly required but is worth setting — it scopes which S3 paths this integration can ever be pointed at from any stage, as a second layer of restriction beyond the IAM policy.

---

## Step 4 — Get Snowflake's AWS identity

```sql
DESC INTEGRATION FINTECH_S3_STORAGE;
```

Pull two values out of the result:

| Property                   | Use                                  |
| -------------------------- | ------------------------------------ |
| `STORAGE_AWS_IAM_USER_ARN` | Goes in the trust policy `Principal` |
| `STORAGE_AWS_EXTERNAL_ID`  | Goes in the trust policy `Condition` |

---

## Step 5 — Patch the IAM role's trust policy

Now replace the placeholder from Step 1 with the real one:

```bash
aws iam update-assume-role-policy \
  --role-name <ROLE_NAME> \
  --policy-document file://trust-policy-final.json
```

`trust-policy-final.json`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "<STORAGE_AWS_IAM_USER_ARN from Step 4>"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "<STORAGE_AWS_EXTERNAL_ID from Step 4>"
        }
      }
    }
  ]
}
```

This is the step that actually connects the two sides. Skipping it (or getting a value wrong) is the #1 cause of `SQL access control error` / `Access Denied` when you later try `LIST @stage`.

---

## Step 6 — Create the file format

```sql
CREATE FILE FORMAT FINTECH.RAW.TRANSACTIONS_CSV
  TYPE = CSV
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"';
```

This has no dependency on anything above — it's pure parsing config, order doesn't matter, but it needs to exist before the stage references it.

---

## Step 7 — Create the external stage

```sql
CREATE STAGE FINTECH.RAW.TRANSACTIONS_STAGE
  URL = 's3://<PROJECT_BUCKET>/raw/transactions/'
  STORAGE_INTEGRATION = FINTECH_S3_STORAGE
  FILE_FORMAT = FINTECH.RAW.TRANSACTIONS_CSV;
```

---

## Step 8 — Validate

```sql
LIST @FINTECH.RAW.TRANSACTIONS_STAGE;
```

If this returns your files: the IAM role, trust policy, permissions policy, storage integration, and stage are all correctly wired. If it errors, check in this order:

1. `DESC INTEGRATION FINTECH_S3_STORAGE` — is it `ENABLED = TRUE`?
2. Does the trust policy principal/external ID **exactly** match what `DESC INTEGRATION` returned (copy-paste, don't retype)?
3. Does the permissions policy resource ARN match the actual bucket/prefix?
4. Is `STORAGE_ALLOWED_LOCATIONS` (if set) covering the stage's URL?

---

## Step 9 — Create the target table

```sql
CREATE TABLE FINTECH.RAW.TRANSACTIONS (
  -- columns matching the PaySim CSV schema
  step INT,
  type STRING,
  amount FLOAT,
  nameOrig STRING,
  oldbalanceOrg FLOAT,
  newbalanceOrig FLOAT,
  nameDest STRING,
  oldbalanceDest FLOAT,
  newbalanceDest FLOAT,
  isFraud INT,
  isFlaggedFraud INT
);
```

(Adjust columns to match your actual PaySim schema/casing.)

---

## Step 10 — Load

```sql
COPY INTO FINTECH.RAW.TRANSACTIONS
FROM @FINTECH.RAW.TRANSACTIONS_STAGE;
```

Check what happened:

```sql
SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME => 'FINTECH.RAW.TRANSACTIONS',
  START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())
));
```

---

## Full command/SQL order, no commentary (quick copy-paste reference)

```bash
aws iam create-role --role-name <ROLE_NAME> --assume-role-policy-document file://trust-policy-placeholder.json
aws iam put-role-policy --role-name <ROLE_NAME> --policy-name <POLICY_NAME> --policy-document file://permissions-policy.json
aws iam get-role --role-name <ROLE_NAME> --query 'Role.Arn' --output text
```

```sql
CREATE STORAGE INTEGRATION FINTECH_S3_STORAGE ...;
DESC INTEGRATION FINTECH_S3_STORAGE;
```

```bash
aws iam update-assume-role-policy --role-name <ROLE_NAME> --policy-document file://trust-policy-final.json
```

```sql
CREATE FILE FORMAT FINTECH.RAW.TRANSACTIONS_CSV ...;
CREATE STAGE FINTECH.RAW.TRANSACTIONS_STAGE ...;
LIST @FINTECH.RAW.TRANSACTIONS_STAGE;
CREATE TABLE FINTECH.RAW.TRANSACTIONS (...);
COPY INTO FINTECH.RAW.TRANSACTIONS FROM @FINTECH.RAW.TRANSACTIONS_STAGE;
```
