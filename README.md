# EventForge

EventForge is a production-oriented event-driven order processing platform
built with Java, Spring Boot, Apache Kafka, and PostgreSQL. It is a distributed
systems case study focused on reliability, consistency, failure recovery,
event contracts, and observability.

## Current Foundation

The foundation currently includes:

- Java 21 and Spring Boot 3.5 Maven baseline
- Four independently runnable Spring Boot services
- Order Service that stores orders and publishes `OrderCreated` from a transactional outbox
- Payment Service that consumes `OrderCreated`, persists simulated outcomes, and publishes payment events
- Actuator health endpoints for every service
- PostgreSQL 16
- Three-node Kafka cluster in KRaft mode
- Kafka Schema Registry
- Avro `OrderCreated` envelope validated by Schema Registry with BACKWARD compatibility
- Initial Kafka topics with six partitions and replication factor three
- Transactional outbox publishers for order and payment events
- Avro `PaymentCompleted` and `PaymentFailed` contracts with separate Schema Registry subjects

## Repository Layout

```text
event-forge/
|-- services/
|   |-- order-service/
|   |-- payment-service/
|   |-- inventory-service/
|   `-- notification-service/
|-- contracts/
|   `-- kafka-schemas/
|-- infrastructure/
|   `-- kafka/
|-- docker-compose.yml
|-- pom.xml
`-- README.md
```

## Prerequisites

- JDK 21
- Maven 3.9 or newer
- Docker Desktop with Docker Compose v2

Ensure these tools are available on `PATH`.

## Start Infrastructure

The Compose file starts PostgreSQL, the Kafka brokers, Schema Registry, the
order and payment services, and an initialization container that creates the
Kafka topics.

```powershell
docker compose up -d
docker compose ps
```

To stop the containers while keeping their data volumes:

```powershell
docker compose down
```

To reset all local infrastructure data:

```powershell
docker compose down -v
```

Default local credentials are defined in `docker-compose.yml`. Copy
`.env.example` to `.env` only when overriding those defaults.
The default Schema Registry compatibility level is `BACKWARD`; configure it with
`SCHEMA_REGISTRY_COMPATIBILITY_LEVEL` when you need to override the local default.

## Run the Services

Run each service in a separate terminal from the repository root:

```powershell
mvn -pl services/order-service spring-boot:run
mvn -pl services/payment-service spring-boot:run
mvn -pl services/inventory-service spring-boot:run
mvn -pl services/notification-service spring-boot:run
```

The default service ports are:

| Service | Port | Health endpoint |
|---|---:|---|
| Order | 8080 | `http://localhost:8080/actuator/health` |
| Payment | 8081 | `http://localhost:8081/actuator/health` |
| Inventory | 8082 | `http://localhost:8082/actuator/health` |
| Notification | 8083 | `http://localhost:8083/actuator/health` |

Each port can be overridden with the `SERVER_PORT` environment variable.

## Infrastructure Ports

| Component | Host port |
|---|---:|
| PostgreSQL | 5432 |
| Kafka broker 1 | 19092 |
| Kafka broker 2 | 19093 |
| Kafka broker 3 | 19094 |
| Schema Registry | 8085 |

The order service connects to the Compose brokers through their internal
listeners. When running it directly on the host, its default bootstrap servers
are `localhost:19092,localhost:19093,localhost:19094`. Override them with
`KAFKA_BOOTSTRAP_SERVERS` as needed.

The outbox publisher polls every second by default, processes up to 100 events
per batch, and waits up to 10 seconds for each Kafka acknowledgement. These can
be configured with `OUTBOX_PUBLISHER_POLL_INTERVAL_MS`,
`OUTBOX_PUBLISHER_BATCH_SIZE`, and `OUTBOX_PUBLISHER_SEND_TIMEOUT_MS`. Publisher
successes, failures, and duration are available through the order and payment
services' `/actuator/metrics` endpoints.

## Payment Flow

After an order is created, the payment consumer in group `payment-service`
processes its `OrderCreated` event asynchronously. It persists the payment and
the resulting `PaymentCompleted` or `PaymentFailed` event in one database
transaction. A separate outbox publisher sends that event to the `payments`
topic with `orderId` as the Kafka key.

The simulated provider approves amounts at or below `1000.00` and declines
amounts above that threshold. Override it with `PAYMENT_SIMULATOR_DECLINE_ABOVE`.
The database's unique constraint on `payments.order_id` and an existence check
prevent duplicate `OrderCreated` deliveries from creating another payment.

Consumer offsets are acknowledged only after the payment transaction commits.
Infrastructure errors retry with a backoff and are not skipped; a failing record
can block its consumer until the problem is resolved. Retry and DLQ topics are
planned for a later phase.

Payment event schemas use `TopicRecordNameStrategy`, registering independently
under `payments-com.eventforge.contracts.payment.PaymentCompleted` and
`payments-com.eventforge.contracts.payment.PaymentFailed`. Both record types can
therefore share `payments` without sharing one compatibility subject.

## Verify Setup

Run the Maven test suite and validate the Compose file before starting the
infrastructure:

```powershell
mvn verify
docker compose config
```

After starting the services, check the health endpoints with `curl.exe` or a
browser. Create an order with a typical amount to produce a completed payment;
use an amount above the configured threshold to produce a failed payment:

```powershell
curl.exe -X POST http://localhost:8080/orders `
  -H "Content-Type: application/json" `
  --data-raw '{ "customerId": "11111111-1111-4111-8111-111111111111", "totalAmount": 149.99, "currency": "USD" }'
curl.exe -X POST http://localhost:8080/orders `
  -H "Content-Type: application/json" `
  --data-raw '{ "customerId": "22222222-2222-4222-8222-222222222222", "totalAmount": 1500.00, "currency": "USD" }'
curl.exe http://localhost:8085/subjects
curl.exe http://localhost:8085/subjects/orders-value/versions/latest
curl.exe http://localhost:8085/subjects/payments-com.eventforge.contracts.payment.PaymentCompleted/versions/latest
curl.exe http://localhost:8085/subjects/payments-com.eventforge.contracts.payment.PaymentFailed/versions/latest

docker compose exec postgres psql -U eventforge -d eventforge -c "SELECT order_id, amount, currency, status, provider_reference, failure_reason FROM payment_service.payments;"
```

## Architectural Direction

EventForge deliberately uses at-least-once delivery, idempotent consumers,
eventual consistency, and a transactional outbox. Kafka ordering will be
preserved per aggregate by using `orderId` as the event key.
