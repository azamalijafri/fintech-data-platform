
# Architecture

## High-Level Flow

Data Producer
    |
    | JSONL transaction files
    v
Amazon S3
    |
    | Stores raw transaction files
    |
    +---------------------> S3 Event Notification
                                  |
                                  | ObjectCreated event
                                  v
                              Amazon SNS
                                  |
                                  | Notification
                                  v
                            Snowpipe
                                  |
                                  | COPY INTO transformation
                                  v
                       Snowflake RAW.TRANSACTIONS


## Component Responsibilities

### 1. Data Producer

The data producer generates transaction events in JSONL format.

Each line represents one transaction.

Example:

{
    "event_id": "...",
    "event_timestamp": "...",
    "step": 2,
    "type": "PAYMENT",
    "amount": 2500.00,
    ...
}

The producer does not communicate directly with Snowflake.

Its responsibility is only to generate transaction data and place the files into Amazon S3.


### 2. Amazon S3

S3 acts as the raw data landing zone.

Bucket:

fintech-data-platform-azam-2026

Transaction files are stored under:

raw/transactions/

Example:

s3://fintech-data-platform-azam-2026/raw/transactions/2026/08/28/new_transactions.jsonl

S3 has two responsibilities:

1. Store the raw transaction files.
2. Generate an ObjectCreated event when a new transaction file arrives.

S3 does not transform the JSONL data.


### 3. S3 Event Notification

The S3 event notification watches the transaction location for new objects.

When a new file arrives:

S3
 |
 | ObjectCreated
 v
SNS Topic

The event contains information such as the bucket and object key.

The event notification does not process the transaction data itself.

Its job is to tell the downstream notification system:

"A new file has arrived."


### 4. Amazon SNS

SNS acts as the notification layer between S3 and Snowflake.

Topic:

fintech-snowpipe-notification

ARN:

arn:aws:sns:ap-south-1:839553328980:fintech-snowpipe-notification

SNS receives the S3 ObjectCreated notification and makes that notification available to Snowpipe.

SNS does not read the JSONL file.

SNS does not transform transaction records.

Its job is event delivery.


### 5. Snowflake Notification Integration

Snowflake object:

FINTECH.RAW.FINTECH_S3_NOTIFICATION

The notification integration tells Snowflake how it interacts with the AWS notification infrastructure.

It contains:

AWS_SNS_TOPIC_ARN
    |
    | Identifies WHERE the notification is sent

AWS_SNS_ROLE_ARN
    |
    | Identifies WHICH AWS IAM role Snowflake uses

The integration connects Snowflake's notification mechanism to the AWS SNS topic.

It is configuration and authorization, not a data-processing component.


### 6. IAM Role for Notification

Role:

fintech-snowpipe-notification-role

Its purpose is to give Snowflake the required AWS permission for the SNS notification operation.

Current permission:

sns:Publish

Resource:

arn:aws:sns:ap-south-1:839553328980:fintech-snowpipe-notification

The permission is scoped to this specific SNS topic.

The role does not provide unrestricted SNS access.


### 7. Snowflake External Stage

Snowflake object:

FINTECH.RAW.TRANSACTIONS_CRED_STAGE

The stage represents the S3 location inside Snowflake.

It points to:

s3://fintech-data-platform-azam-2026/raw/transactions/

The stage is the bridge between Snowflake and the files stored in S3.

It does not copy the data into Snowflake by itself.

It provides Snowflake with a reference to the external files.


### 8. Snowflake File Format

Snowflake object:

FINTECH.RAW.JSONL_FORMAT

Type:

JSON

The file format defines how Snowflake interprets the files stored in the stage.

It contains parsing configuration such as:

- File type
- Date and time formats
- Compression
- NULL handling
- UTF-8 handling
- Multiline behavior
- Duplicate-key handling

The stage references this file format:

TRANSACTIONS_CRED_STAGE
        |
        v
JSONL_FORMAT

Therefore the Snowpipe COPY statement does not need to repeatedly specify the JSON file format.


### 9. Snowpipe

Snowflake object:

FINTECH.RAW.TRANSACTIONS_PIPE

Snowpipe is responsible for automatically loading newly arrived files into the target Snowflake table.

Its COPY INTO definition performs two jobs:

1. Read the JSON records from the stage.
2. Transform JSON fields into the target table columns.

For example:

$1:step::NUMBER
$1:type::VARCHAR
$1:amount::NUMBER(18,2)

Here:

$1

represents the JSON record.

$1:amount

extracts the amount field.

::NUMBER(18,2)

converts it into the required Snowflake data type.

The result is inserted into:

FINTECH.RAW.TRANSACTIONS


### 10. Snowflake Target Table

Table:

FINTECH.RAW.TRANSACTIONS

This is the structured destination for the transaction data.

Snowpipe converts the semi-structured JSON records into the table's relational columns.

Example:

JSON:

{
    "step": 2,
    "type": "PAYMENT",
    "amount": 2500.00
}

becomes:

STEP   = 2
TYPE   = PAYMENT
AMOUNT = 2500.00


## Complete Interaction

When a new transaction file arrives:

1. The data producer creates a JSONL file.

2. The file is uploaded to:

   Amazon S3
   |
   raw/transactions/

3. S3 stores the raw file.

4. S3 generates an ObjectCreated event.

5. The S3 event is sent to:

   Amazon SNS
   |
   fintech-snowpipe-notification

6. Snowflake's notification integration provides the AWS/Snowflake connection required for the notification mechanism.

7. Snowpipe receives the notification that a new file is available.

8. Snowpipe accesses the external stage.

9. The stage points Snowflake to the S3 file.

10. The stage uses JSONL_FORMAT to interpret the file.

11. Snowpipe executes its COPY INTO definition.

12. JSON fields are extracted and cast to the target data types.

13. The transformed records are inserted into:

   FINTECH.RAW.TRANSACTIONS


## Responsibility Boundaries

Data Producer
    -> Generate transaction data

S3
    -> Store raw files
    -> Detect new objects

S3 Event Notification
    -> Generate delivery event

SNS
    -> Deliver the event

Notification Integration
    -> Configure Snowflake's AWS notification connection

IAM Role
    -> Authorize the required AWS operation

External Stage
    -> Represent the S3 location inside Snowflake

File Format
    -> Define how Snowflake interprets JSONL

Snowpipe
    -> Detect/process new files
    -> Execute COPY transformation

RAW.TRANSACTIONS
    -> Store structured transaction records


