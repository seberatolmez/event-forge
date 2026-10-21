# Kafka Infrastructure

The local environment runs a three-node Kafka cluster in KRaft mode using the Confluent Kafka
and Schema Registry images. The brokers use plaintext listeners because this
topology is for local development only.

The Compose setup creates the initial topics with six partitions and a
replication factor of three:

- `orders`
- `payments`
- `inventory`
- `notifications`
- `payments.retry`
- `payments.dlq`
- `inventory.retry`
- `inventory.dlq`

Order-related events use `orderId` as their Kafka key so events for the same
order remain partitioned together. Topic provisioning is kept in the
infrastructure layer so services can start with a known Kafka topology.

The Payment Service writes both `PaymentCompleted` and `PaymentFailed` to the
`payments` topic with `orderId` as the record key. It uses
`TopicRecordNameStrategy`, so the event contracts register under separate
subjects and evolve independently.

The Inventory Service writes `InventoryReserved` and
`InventoryReservationFailed` to the `inventory` topic with `orderId` as the
record key. It uses `TopicRecordNameStrategy` so both contracts have separate
Schema Registry subjects.
