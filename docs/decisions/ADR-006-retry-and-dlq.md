# ADR-006: Retry and Dead Letter Queue

## Status

Accepted (payment-service and inventory-service consumers).

## Context

Consumers used to retry every exception forever. A deterministic failure (an invalid event) blocked its partition
indefinitely, and a transient failure blocked the whole `orders` partition while it was retried.

## Decision

Each consumer has a main listener on `orders` and a retry listener on `<domain>.retry`. Topic names are per consumer
(`payments.retry`, `payments.dlq`, `inventory.retry`, `inventory.dlq`) because payment and inventory both consume
`orders`; names derived from the source topic would collide.

```text
orders -> main listener -> success
             | transient: 2 quick in-place retries, then publish to <domain>.retry
             | permanent: publish straight to <domain>.dlq
<domain>.retry -> retry listener -> success
             | still failing after 3 attempts (exponential backoff), or permanent: <domain>.dlq
```

### Error classification

Permanent (never retried, dead-lettered immediately): `IllegalArgumentException` (contract and validation violations),
undeserializable records, `ClassCastException`, and payment's `InvalidPayment*Exception`. Everything else, such as
database or Kafka errors, is transient. The list lives next to the error handler in each service's
`*KafkaConfiguration`.

Deserialization failures are caught by `ErrorHandlingDeserializer` so they reach the error handler instead of
crashing the consumer inside `poll()`. Their original bytes are republished to the DLQ.

### DLQ metadata

Spring Kafka's `DeadLetterPublishingRecoverer` headers (original topic, partition, offset, exception class and
message, stack trace, timestamp) plus:

| Header | Meaning |
|---|---|
| `eventId` | `eventId` of the failed event (absent if it could not be deserialized) |
| `consumer` | service that dead-lettered the record |
| `retryCount` | number of retry-topic passes (`1` after the retry topic, absent if dead-lettered directly) |

The payload is copied unchanged. For a record that reached the DLQ from the retry topic, the `kafka_dlt-original-*`
headers describe the retry-topic hop; the `orders` hop is the first value of each repeated header.

## Consequences

- A poison message no longer blocks `orders`; a slow transient failure only delays the retry topic.
- Offsets of recovered records are committed (`commitRecovered`), so a restart does not republish them.
- Delivery stays at-least-once; consumers remain idempotent through `processed_events`.
- Known limits: the retry topic has no delay before its first attempt; there is no DLQ replay tool and no retry or DLQ
  metrics (Phases 10+/14); notification-service still skips undeserializable records without a DLQ.
