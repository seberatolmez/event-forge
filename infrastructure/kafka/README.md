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

Business events use the aggregate identifier as their Kafka key. Topic
provisioning is kept in the infrastructure layer so services can start with a
known Kafka topology.
