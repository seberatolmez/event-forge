# EventForge

EventForge is a production-oriented event-driven order processing platform
built with Java, Spring Boot, Apache Kafka, and PostgreSQL. It is a distributed
systems case study focused on reliability, consistency, failure recovery,
event contracts, and observability.

## Current Foundation

The foundation currently includes:

- Java 21 and Spring Boot 3.5 Maven baseline
- Four independently runnable Spring Boot service skeletons
- Actuator health endpoints for every service
- PostgreSQL 16
- Three-node Kafka cluster in KRaft mode
- Kafka Schema Registry
- Initial Kafka topics with six partitions and replication factor three

## Repository Layout

```text
event-forge/
|-- services/
|   |-- order-service/
|   |-- payment-service/
|   |-- inventory-service/
|   `-- notification-service/
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

The Compose file starts PostgreSQL, the Kafka brokers, Schema Registry, and an
initialization container that creates the Kafka topics.

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

## Verify Setup

Run the Maven test suite and validate the Compose file before starting the
infrastructure:

```powershell
mvn verify
docker compose config
```

After starting the services, check the health endpoints with `curl.exe` or a
browser. Schema Registry should return an empty subject list until event
contracts are introduced:

```powershell
curl.exe http://localhost:8085/subjects
```

## Architectural Direction

EventForge deliberately uses at-least-once delivery, idempotent consumers,
eventual consistency, and a transactional outbox. Kafka ordering will be
preserved per aggregate by using `orderId` as the event key.
