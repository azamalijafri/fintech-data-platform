# S3 Notification → Snowpipe Auto-Trigger — Setup Runbook

Personal reference: exact order of operations for wiring auto-ingest. Assumes the storage integration + stage + file format from `SNOWFLAKE_S3_STORAGE_INTEGRATION.md` already exist — this doc only adds the event-driven trigger on top.

Same back-and-forth pattern as the storage integration: the SNS topic needs to exist before Snowflake can point at it, and Snowflake's principal isn't known until _after_ the notification integration is created — so the topic policy gets patched in two passes.

---

## Order of operations (high level)

```
1. Create the SNS topic
2. Add the S3-publish permission to the topic policy (you already know this principal: s3.amazonaws.com)
3. Create the Snowflake notification integration, pointing at the topic ARN
4. DESC the integration → get Snowflake's AWS principal
5. Go back to AWS → add the Snowflake-subscribe permission to the topic policy
6. Configure the S3 bucket notification to publish to the topic
7. Create the pipe (AUTO_INGEST = TRUE)
8. Validate: drop a file in S3, check pipe status / copy history
9. (If needed) backfill files that already existed before the pipe was created
```

---

## Step 1 — Create the SNS topic

```bash
aws sns create-topic --name <TOPIC_NAME> --region <AWS_REGION>
```

Save the returned `TopicArn` — you need it everywhere below.

---

## Step 2 — Allow S3 to publish (first half of the topic policy)

```bash
aws sns set-topic-attributes \
  --topic-arn <SNS_TOPIC_ARN> \
  --attribute-name Policy \
  --attribute-value file://topic-policy.json \
  --region <AWS_REGION>
```

`topic-policy.json` (S3-publish grant only, for now):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3Publish",
      "Effect": "Allow",
      "Principal": { "Service": "s3.amazonaws.com" },
      "Action": "sns:Publish",
      "Resource": "<SNS_TOPIC_ARN>",
      "Condition": {
        "StringEquals": { "aws:SourceAccount": "<AWS_ACCOUNT_ID>" },
        "ArnEquals": { "aws:SourceArn": "arn:aws:s3:::<PROJECT_BUCKET>" }
      }
    }
  ]
}
```

---

## Step 3 — Create the Snowflake notification integration

```sql
CREATE NOTIFICATION INTEGRATION FINTECH_S3_NOTIFICATION
  ENABLED = TRUE
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = AWS_SNS
  DIRECTION = INBOUND
  AWS_SNS_TOPIC_ARN = '<SNS_TOPIC_ARN>';
```

---

## Step 4 — Get Snowflake's AWS identity for this integration

```sql
DESC NOTIFICATION INTEGRATION FINTECH_S3_NOTIFICATION;
```

Pull out:

| Property              | Use                                                                                                                                          |
| --------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| `SF_AWS_IAM_USER_ARN` | Goes in the topic policy `Principal` for the subscribe grant                                                                                 |
| `SF_AWS_EXTERNAL_ID`  | Not used in the topic policy itself (unlike the storage integration) — Snowflake handles this internally for the SQS subscription it creates |

(Note: this is a _different_ principal/mechanism from the storage integration's `STORAGE_AWS_IAM_USER_ARN` — don't reuse values across the two docs, even though the pattern looks identical.)

---

## Step 5 — Allow Snowflake to subscribe (second half of the topic policy)

Re-run `set-topic-attributes` with **both** statements now — you're replacing the whole policy document, not appending, so include Step 2's statement again:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3Publish",
      "Effect": "Allow",
      "Principal": { "Service": "s3.amazonaws.com" },
      "Action": "sns:Publish",
      "Resource": "<SNS_TOPIC_ARN>",
      "Condition": {
        "StringEquals": { "aws:SourceAccount": "<AWS_ACCOUNT_ID>" },
        "ArnEquals": { "aws:SourceArn": "arn:aws:s3:::<PROJECT_BUCKET>" }
      }
    },
    {
      "Sid": "SnowflakeSubscribe",
      "Effect": "Allow",
      "Principal": { "AWS": "<SF_AWS_IAM_USER_ARN from Step 4>" },
      "Action": "sns:Subscribe",
      "Resource": "<SNS_TOPIC_ARN>"
    }
  ]
}
```

```bash
aws sns set-topic-attributes \
  --topic-arn <SNS_TOPIC_ARN> \
  --attribute-name Policy \
  --attribute-value file://topic-policy-final.json \
  --region <AWS_REGION>
```

Snowflake creates its own SQS subscription against this topic automatically once it has `sns:Subscribe` — there's nothing to manually subscribe on your end. Confirm it appeared:

```bash
aws sns list-subscriptions-by-topic --topic-arn <SNS_TOPIC_ARN> --region <AWS_REGION>
```

You should see a subscription with protocol `sqs` and an endpoint ARN in what looks like Snowflake's AWS account, not yours.

---

## Step 6 — Configure the S3 bucket notification

```bash
aws s3api put-bucket-notification-configuration \
  --bucket <PROJECT_BUCKET> \
  --notification-configuration file://bucket-notification.json \
  --region <AWS_REGION>
```

`bucket-notification.json`:

```json
{
  "TopicConfigurations": [
    {
      "TopicArn": "<SNS_TOPIC_ARN>",
      "Events": ["s3:ObjectCreated:*"],
      "Filter": {
        "Key": {
          "FilterRules": [{ "Name": "Prefix", "Value": "raw/transactions/" }]
        }
      }
    }
  ]
}
```

This must come **after** Step 2 (S3-publish grant exists) — S3 validates it can publish to the topic when you set this, and rejects the config otherwise.

---

## Step 7 — Create the pipe

```sql
CREATE PIPE FINTECH.RAW.TRANSACTIONS_PIPE
  AUTO_INGEST = TRUE
AS
COPY INTO FINTECH.RAW.TRANSACTIONS
FROM @FINTECH.RAW.TRANSACTIONS_STAGE;
```

`AUTO_INGEST = TRUE` is what makes Snowflake associate this pipe with the notification integration tied to the stage's S3 location — there's no explicit `NOTIFICATION_INTEGRATION = ...` clause to set; it's inferred from the stage.

---

## Step 8 — Validate end-to-end

Drop a real test file into `s3://<PROJECT_BUCKET>/raw/transactions/` and then check:

```sql
-- Is the pipe alive and how many messages pending
SELECT SYSTEM$PIPE_STATUS('FINTECH.RAW.TRANSACTIONS_PIPE');

-- Did it actually load
SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME => 'FINTECH.RAW.TRANSACTIONS',
  START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())
));
```

If nothing loads, check in this order:

1. `aws sns list-subscriptions-by-topic` — did Snowflake's SQS subscription actually get created (Step 5)?
2. `aws s3api get-bucket-notification-configuration --bucket <PROJECT_BUCKET>` — is the notification config actually saved (Step 6)?
3. Did the test file's key actually match the `raw/transactions/` prefix?
4. `SYSTEM$PIPE_STATUS` — is `executionState` something other than `RUNNING`?

---

## Step 9 — Backfill files that predate the pipe

Auto-ingest only reacts to **new** `ObjectCreated` events from the moment the bucket notification was configured (Step 6) onward. Files already sitting in the bucket before that won't trigger automatically. To load them:

```sql
ALTER PIPE FINTECH.RAW.TRANSACTIONS_PIPE REFRESH;
```

or, for a one-off manual load of everything currently in the stage regardless of pipe state:

```sql
COPY INTO FINTECH.RAW.TRANSACTIONS
FROM @FINTECH.RAW.TRANSACTIONS_STAGE;
```

---

## Full command/SQL order, no commentary (quick copy-paste reference)

```bash
aws sns create-topic --name <TOPIC_NAME> --region <AWS_REGION>
aws sns set-topic-attributes --topic-arn <SNS_TOPIC_ARN> --attribute-name Policy --attribute-value file://topic-policy.json --region <AWS_REGION>
```

```sql
CREATE NOTIFICATION INTEGRATION FINTECH_S3_NOTIFICATION ...;
DESC NOTIFICATION INTEGRATION FINTECH_S3_NOTIFICATION;
```

```bash
aws sns set-topic-attributes --topic-arn <SNS_TOPIC_ARN> --attribute-name Policy --attribute-value file://topic-policy-final.json --region <AWS_REGION>
aws sns list-subscriptions-by-topic --topic-arn <SNS_TOPIC_ARN> --region <AWS_REGION>
aws s3api put-bucket-notification-configuration --bucket <PROJECT_BUCKET> --notification-configuration file://bucket-notification.json --region <AWS_REGION>
```

```sql
CREATE PIPE FINTECH.RAW.TRANSACTIONS_PIPE AUTO_INGEST = TRUE AS COPY INTO ...;
SELECT SYSTEM$PIPE_STATUS('FINTECH.RAW.TRANSACTIONS_PIPE');
ALTER PIPE FINTECH.RAW.TRANSACTIONS_PIPE REFRESH;  -- backfill pre-existing files
```
