# Logical Replication Stream

## Summary
Read the AWS doc `docs/streaming/pg_cdc.txt` in full to understand the scope of the project. The goal is think ultra hard in order to modify the Python consumer in the "Consume the replication stream" section of the documentation to be optimized for the lowest latency possible. You are tasked with creating an scaleable Python application that connects to the DB cluster endpoint, starts consuming the replication stream, and forwards each record to the Kinesis data stream.

## Assumptions
- Application deployed in an existing AWS EKS cluster with configuration flexability
- All services/resources are within the same region and account

## Architecture Decisions
- Kinesis: The kinesis partitionKey should be the primaryKey of the CDC table
    - If parsing the message to get the primaryKey of the CDC table significantly increases latency then provide an alternative for the kinesis stream
- Psycopg3:  Has built-in, first-class asyncio support
- Replication Slot: Do not drop the replication slot only attempt to create if it doesn't exist
- State management: Must acknowledge and flush messages after records are pushed to Kinesis
- PyDantic: Use pydantic for settings

## Research
- Use context7 libraryId /websites/psycopg_psycopg3 for replication options
    - format-version: 2
    - include-timestamp: true
    - include-lsn: true
    - include-transactions: false
    - include-pk: true

## Avoid
- Schema evolution
- Alerting
- Data tranformation
