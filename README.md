# Kafka Router Worker

Kafka Router Worker is a learning-oriented, production-inspired .NET microservice that consumes events from Kafka, routes them dynamically using MongoDB rules, supports idempotency, handles invalid messages through a dead-letter topic, exposes operational endpoints, and is tested with real infrastructure using Testcontainers.

The project was built as a complete hands-on course to understand how event-driven backend services can be designed, implemented, tested, operated, and reviewed.

## What This Project Demonstrates

This repository demonstrates several important backend engineering patterns:

- Kafka consumer and producer integration
- Dynamic event routing based on MongoDB rules
- Dead-letter queue handling
- Application failure vs technical failure classification
- Manual Kafka offset commit
- Idempotency with MongoDB
- Technical retry with Polly
- Structured logging with correlation IDs
- Health checks
- Metrics endpoint
- Diagnostic endpoints
- Docker Compose local environment
- Horizontal scaling with multiple Worker instances
- Unit tests
- HTTP integration tests
- MongoDB integration tests with Testcontainers
- Kafka integration tests with Testcontainers
- Controlled end-to-end test
- Operational runbook
- Architecture review
- Production readiness checklist

## High-Level Architecture

```text
              ┌────────────────────┐
              │ Upstream Producer  │
              └─────────┬──────────┘
                        │
                        ▼
              ┌────────────────────┐
              │ Kafka input topic  │
              │ events.inbound     │
              └─────────┬──────────┘
                        │
                        ▼
              ┌────────────────────┐
              │ Kafka Router       │
              │ Worker             │
              └──────┬───────┬─────┘
                     │       │
          reads rules│       │stores processed IDs
                     ▼       ▼
          ┌──────────────┐ ┌────────────────────┐
          │ MongoDB      │ │ MongoDB            │
          │ routing_rules│ │ processed_messages │
          └──────────────┘ └────────────────────┘
                     │
                     ▼
       ┌──────────────────────────────────┐
       │ Destination Kafka topics         │
       │ events.crm                       │
       │ events.billing                   │
       │ events.notifications             │
       └──────────────────────────────────┘

       Invalid or unroutable messages
                     │
                     ▼
              ┌────────────────────┐
              │ events.dead-letter │
              └────────────────────┘
```

## Processing Pipeline

The core processing pipeline is:

```text
consume
parse
deduplicate
route
produce
record
commit
observe
```

A message is consumed from the inbound Kafka topic, parsed into an event envelope, checked for duplicates, routed through MongoDB rules, produced to destination topics, stored as processed, committed, and tracked through logs and metrics.

## Main Technologies

| Area | Technology |
|---|---|
| Runtime | .NET |
| Application type | Worker Service hosted as ASP.NET Core app |
| Messaging | Apache Kafka |
| Kafka client | Confluent.Kafka |
| Database | MongoDB |
| Retry | Polly |
| Tests | xUnit, Moq, FluentAssertions |
| Integration tests | WebApplicationFactory, Testcontainers |
| Containers | Docker, Docker Compose |

## Main Kafka Topics

| Topic | Purpose |
|---|---|
| `events.inbound` | Main input topic |
| `events.crm` | CRM destination topic |
| `events.billing` | Billing destination topic |
| `events.notifications` | Notifications destination topic |
| `events.dead-letter` | Dead-letter topic |

## Main MongoDB Collections

| Collection | Purpose |
|---|---|
| `routing_rules` | Stores enabled routing rules by event type |
| `processed_messages` | Stores processed event IDs for idempotency |

## Event Envelope

Example input message:

```json
{
  "eventId": "manual-test-001",
  "eventType": "CustomerCreated",
  "occurredAt": "2026-05-20T10:00:00Z",
  "source": "manual-test",
  "correlationId": "manual-correlation-001",
  "payload": {
    "customerId": "CUST-001",
    "email": "test@example.com"
  }
}
```

Required fields:

```text
eventId
eventType
occurredAt
payload
```

Optional fields:

```text
source
correlationId
```

## Failure Handling Model

The service distinguishes between application failures, technical failures, duplicates, and successful processing.

| Scenario | Behavior |
|---|---|
| Valid message | Routed to destination topics |
| Invalid JSON | Sent to dead-letter topic |
| Missing required field | Sent to dead-letter topic |
| Missing routing rule | Sent to dead-letter topic |
| Empty destination topics | Sent to dead-letter topic |
| Duplicate eventId | Skipped and committed |
| Technical failure | Retried |

Application failures are not retried because retrying the same invalid message would normally produce the same result.

Technical failures are retried because they may be transient.

## Idempotency

The Worker stores processed event IDs in MongoDB.

Expected behavior:

```text
first message with eventId X
  -> route message
  -> insert X into processed_messages
  -> commit Kafka offset

second message with eventId X
  -> detect duplicate
  -> do not route again
  -> commit Kafka offset
```

This protects downstream systems from duplicate side effects.

## Manual Kafka Commit

The Worker uses manual Kafka offset commit.

Offsets are committed only after a safe processing decision:

| Scenario | Offset committed? |
|---|---:|
| Message successfully routed | Yes |
| Message sent to DLQ | Yes |
| Duplicate message skipped | Yes |
| Technical failure before safe completion | No |

This is one of the most important design choices in the project.

## Operational Endpoints

| Endpoint | Purpose |
|---|---|
| `/health/live` | Checks if the process is alive |
| `/health/ready` | Checks if Kafka and MongoDB are available |
| `/metrics` | Returns in-memory metrics |
| `/diagnostics/config` | Returns sanitized runtime configuration |
| `/diagnostics/routing-rules` | Returns enabled routing rules |
| `/diagnostics/status` | Returns operational status summary |

## Running Locally

### Prerequisites

- .NET SDK
- Docker
- Docker Compose

### Start the local environment

```bash
cd infra
docker compose up --build --scale kafka-router-worker=3
```

### Stop the local environment

```bash
cd infra
docker compose down
```

### Check running containers

```bash
cd infra
docker compose ps
```

Container names may vary depending on the Docker Compose project name. Use `docker compose ps` to verify the actual names.

## Useful Commands

### Check Worker readiness

```bash
docker exec infra-kafka-router-worker-1 curl -i http://localhost:8080/health/ready
```

### Check diagnostics

```bash
docker exec infra-kafka-router-worker-1 curl -s http://localhost:8080/diagnostics/status
```

### Check metrics

```bash
docker exec infra-kafka-router-worker-1 curl -s http://localhost:8080/metrics
```

### List Kafka topics

```bash
docker exec kafka-router-broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

### Describe consumer group

```bash
docker exec kafka-router-broker /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group kafka-router-worker-local
```

### Produce a valid test message

```bash
docker exec -i kafka-router-broker /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic events.inbound <<'EOF'
{"eventId":"manual-test-001","eventType":"CustomerCreated","occurredAt":"2026-05-20T10:00:00Z","source":"manual-test","correlationId":"manual-correlation-001","payload":{"customerId":"CUST-001","email":"test@example.com"}}
EOF
```

### Consume routed CRM events

```bash
docker exec kafka-router-broker /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic events.crm \
  --from-beginning \
  --timeout-ms 10000
```

### Consume dead-letter events

```bash
docker exec kafka-router-broker /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic events.dead-letter \
  --from-beginning \
  --timeout-ms 10000
```

## Build and Test

### Build

```bash
dotnet build
```

### Run all tests

```bash
dotnet test
```

The test suite includes:

- unit tests
- HTTP integration tests
- MongoDB integration tests with Testcontainers
- Kafka integration tests with Testcontainers
- controlled end-to-end test

## Documentation

The repository includes several documentation files:

| Document | Purpose |
|---|---|
| `docs/CourseBook.md` | Main learning document |
| `docs/Runbook.md` | Operational troubleshooting guide |
| `docs/ArchitectureReview.md` | Architecture and trade-off analysis |
| `docs/ProductionReadinessChecklist.md` | Production readiness assessment |
| `docs/FinalReview.md` | Final project review |

## Production Readiness

This project is a strong learning-grade and local integration-grade system.

It is suitable for:

```text
learning
experimentation
technical demos
architecture discussions
local integration testing
```

It is not production-ready without additional hardening.

Main missing production-grade areas:

```text
external observability
centralized logging
distributed tracing
secret management
Kafka/MongoDB authentication and TLS
protected diagnostic endpoints
CI/CD
deployment manifests
load testing
DLQ reprocessing tooling
retention policies
backup/restore strategy
```

## Recommended Future Improvements

Recommended hardening order:

```text
1. Add CI pipeline.
2. Add OpenTelemetry.
3. Export metrics and traces externally.
4. Centralize logs.
5. Protect diagnostic endpoints.
6. Move secrets to a secret store.
7. Add Kafka and MongoDB authentication/TLS.
8. Add processed_messages retention policy.
9. Add consumer lag monitoring.
10. Add DLQ replay tooling.
11. Add load tests.
12. Add Kubernetes manifests or target deployment templates.
```

## License

This project is licensed under the MIT License.

Copyright (c) 2026 Francesco Paolo Piga.

You are free to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of this software, provided that the copyright notice and license text are preserved in all copies or substantial portions of the software.

See the [LICENSE](./LICENSE) file for details.

## Attribution and Citation

If you use this project, please credit:

```text
Kafka Router Worker
Author: Francesco Paolo Piga
GitHub: https://github.com/snapehalfbloodprince
License: MIT
```

For academic, educational, or public references, please use the citation metadata provided in `CITATION.cff`.

## Author

Francesco Paolo Piga  
GitHub: [@snapehalfbloodprince](https://github.com/snapehalfbloodprince)

## Final Note

A reliable backend system is not just code that works in the happy path.

It is code that behaves predictably when unhappy paths happen.
