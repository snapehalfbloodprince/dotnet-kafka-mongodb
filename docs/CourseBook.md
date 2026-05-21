# Kafka Router Worker Course Book

Related documents:

- [Operational Runbook](./Runbook.md)
- [Architecture Review](./ArchitectureReview.md)
- [Production Readiness Checklist](./ProductionReadinessChecklist.md)

---

## 1. Introduction

This document summarizes the design, implementation, testing, and operational model of the Kafka Router Worker project.

The project is a practical learning path for building a horizontally scalable .NET microservice that consumes messages from Kafka, routes them dynamically based on MongoDB rules, handles failures safely, supports idempotency, exposes operational endpoints, and is tested with real infrastructure through Testcontainers.

The final goal of the project is not only to build a working service, but to understand the engineering reasoning behind it.

---

## 2. Project Goal

The goal of the Kafka Router Worker is to process inbound events from Kafka and route them to one or more destination topics.

At a high level, the service performs this pipeline:

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

The service must:

```text
- consume messages from an inbound Kafka topic
- parse messages into a known event envelope
- validate required envelope fields
- read routing rules from MongoDB
- produce the original event to one or more destination topics
- send invalid or unroutable messages to a dead-letter topic
- avoid duplicate processing through MongoDB idempotency
- expose health, metrics, and diagnostic endpoints
- support multiple Worker instances in the same consumer group
- be testable locally using Docker and Testcontainers
```

---

## 3. Technology Stack

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

---

## 4. Main Architecture

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

The Worker is intentionally modular.

Each major responsibility is isolated into a dedicated component.

| Responsibility | Component |
|---|---|
| Runtime loop | `Worker` |
| Processing orchestration | `MessageProcessingService` |
| Retry orchestration | `MessageProcessingRetryService` |
| Kafka consumption | `KafkaMessageConsumer` |
| Kafka production | `KafkaMessageProducer` |
| Event parsing | `EventEnvelopeParser` |
| Routing | `MongoDbEventRoutingService` |
| DLQ payload creation | `DeadLetterMessageFactory` |
| Routing rules persistence | `RoutingRuleRepository` |
| Idempotency persistence | `ProcessedMessageRepository` |
| Metrics | `InMemoryWorkerMetrics` |
| Health checks | Kafka and MongoDB health check services |
| Diagnostics | Minimal API diagnostic endpoints |

---

## 5. Event Envelope

The expected input message is a JSON event envelope.

Example:

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

Optional but useful fields:

```text
source
correlationId
```

The parser validates the envelope and returns explicit error codes for application failures.

Important parser error codes include:

```text
EMPTY_MESSAGE
INVALID_JSON
NULL_EVENT
MISSING_EVENT_ID
MISSING_EVENT_TYPE
MISSING_OCCURRED_AT
MISSING_PAYLOAD
```

---

## 6. Routing Model

Routing is dynamic and stored in MongoDB.

The main collection is:

```text
routing_rules
```

A routing rule maps an `eventType` to one or more Kafka destination topics.

Example:

```json
{
  "eventType": "CustomerCreated",
  "destinationTopics": ["events.crm"],
  "isEnabled": true
}
```

Routing outcomes:

| Situation | Outcome |
|---|---|
| Rule exists and has valid destination topics | Produce to destination topics |
| Rule does not exist | Send to DLQ |
| Rule has empty destination topics | Send to DLQ |
| Event type missing | Send to DLQ |

Important routing error codes:

```text
ROUTING_RULE_NOT_FOUND
EMPTY_DESTINATION_TOPICS
MISSING_EVENT_TYPE
```

---

## 7. Dead-Letter Queue

Invalid or unroutable messages are sent to:

```text
events.dead-letter
```

The DLQ exists to preserve failed messages and make failures inspectable.

DLQ messages contain:

```text
original topic
original partition
original offset
original key
error code
error message
failed timestamp
eventId if available
eventType if available
correlationId if available
original payload
```

The DLQ is used for application failures, not for every technical exception.

Application failures are not retried because retrying the same invalid message would normally produce the same result.

Examples:

| Failure | Error Code | Retry? |
|---|---|---:|
| Invalid JSON | `INVALID_JSON` | No |
| Missing eventId | `MISSING_EVENT_ID` | No |
| Missing routing rule | `ROUTING_RULE_NOT_FOUND` | No |
| Empty destination topics | `EMPTY_DESTINATION_TOPICS` | No |

---

## 8. Idempotency

The service protects downstream systems from duplicate event processing.

The main collection is:

```text
processed_messages
```

The Worker checks whether an `eventId` already exists before routing the message.

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

Idempotency is important because in distributed systems duplicate delivery can happen.

Kafka producer idempotence and application-level idempotency are different concepts:

| Type | Protects Against |
|---|---|
| Kafka producer idempotence | Duplicate records caused by producer retries |
| Application idempotency | Duplicate business events with the same `eventId` |

---

## 9. Kafka Offset Strategy

The Worker uses manual Kafka offset commit.

The offset is committed only after a safe processing decision.

| Scenario | Offset committed? |
|---|---:|
| Message successfully routed | Yes |
| Message sent to DLQ | Yes |
| Duplicate message skipped | Yes |
| Technical failure before safe completion | No |

This is one of the most important design choices in the project.

It prevents message loss during technical failures.

---

## 10. Failure Classification

The project separates failures into two main categories.

### Application failures

Application failures are caused by message content or routing configuration.

Examples:

```text
invalid JSON
missing required envelope fields
missing routing rule
empty destination topics
```

Application failures are sent to the DLQ and the offset is committed.

### Technical failures

Technical failures are caused by infrastructure, connectivity, or unexpected runtime exceptions.

Examples:

```text
Kafka temporarily unavailable
MongoDB timeout
network instability
unexpected exception
```

Technical failures are retried by the retry service.

---

## 11. Retry Model

Retry logic is isolated in:

```text
MessageProcessingRetryService
```

The project uses Polly for retry behavior.

Retry settings are configurable through `WorkerOptions`.

Important settings:

```text
TechnicalRetryMaxAttempts
TechnicalRetryInitialDelayInSeconds
TechnicalRetryMaxDelayInSeconds
ConsecutiveFailuresWarningThreshold
```

The reason retry logic is separated from processing logic is maintainability.

Processing logic should answer:

```text
what does it mean to process a message?
```

Retry logic should answer:

```text
what do we do if processing fails technically?
```

---

## 12. Horizontal Scalability

The Worker can run multiple instances using the same Kafka consumer group.

Example local command:

```bash
cd infra
docker compose up --build --scale kafka-router-worker=3
```

Kafka distributes partitions among consumers in the same group.

Important principle:

```text
The maximum useful parallelism is limited by the number of partitions in the input topic.
```

If `events.inbound` has one partition, only one Worker instance can actively consume that partition at a time.

Scaling Workers helps only when Kafka partitions allow parallelism.

---

## 13. Health Checks

The project exposes two main health endpoints:

| Endpoint | Purpose |
|---|---|
| `/health/live` | Checks if the process is alive |
| `/health/ready` | Checks if the service can safely work |

Liveness and readiness are intentionally different.

```text
liveness = the process exists
readiness = the process can safely process messages
```

If MongoDB or Kafka are unavailable, the service may still be alive, but it should not be ready.

---

## 14. Metrics

The project exposes:

```text
/metrics
```

Metrics are currently in-memory.

Tracked values include:

```text
processed messages
dead-letter messages
duplicate messages
technical failures
last processed event
last dead-letter event
last duplicate event
total processing duration
average processing duration
max processing duration
```

This is useful locally and for learning.

Production systems should export metrics externally using a real telemetry stack.

---

## 15. Diagnostic Endpoints

The project exposes diagnostic endpoints:

| Endpoint | Purpose |
|---|---|
| `/diagnostics/config` | Sanitized runtime configuration |
| `/diagnostics/routing-rules` | Enabled routing rules |
| `/diagnostics/status` | Kafka, MongoDB, and metrics summary |

The MongoDB connection string is sanitized before being returned.

Diagnostic endpoints are useful locally, but in production they must be protected or restricted.

---

## 16. Docker Environment

The local environment includes:

```text
Kafka
MongoDB
Mongo Express
Kafka Router Worker
```

Useful commands:

```bash
cd infra
docker compose up --build --scale kafka-router-worker=3
```

```bash
cd infra
docker compose down
```

```bash
cd infra
docker compose ps
```

Container names may vary depending on the Docker Compose project name.

Always verify actual names with:

```bash
docker compose ps
```

---

## 17. Testing Strategy

The project uses multiple layers of testing.

### Unit tests

Unit tests cover isolated behavior:

```text
parser
routing decisions
metrics
validators
message processing branches
retry service
connection string sanitizer
```

### HTTP integration tests

HTTP integration tests use `WebApplicationFactory`.

They verify:

```text
health endpoints
metrics endpoint
diagnostic endpoints
application wiring
```

External dependencies are mocked in this layer.

### MongoDB integration tests

MongoDB integration tests use Testcontainers.

They verify repositories against a real MongoDB instance.

This catches issues such as:

```text
wrong collection name
wrong index
duplicate key behavior
serialization mismatch
query mismatch
```

### Kafka integration tests

Kafka integration tests use Testcontainers with:

```text
apache/kafka-native:3.8.0
```

This image was selected because it works reliably on Apple Silicon for this project.

### Controlled end-to-end test

The controlled E2E test uses:

```text
real Kafka
real MongoDB
real consumer
real producer
real parser
real routing
real message processing
real metrics
```

It does not start the infinite background Worker loop.

This is intentional: it provides high confidence while keeping the test deterministic.

---

## 18. Important Lessons Learned

### Kafka consumer groups

A consumer group allows multiple service instances to share the work of consuming messages from a topic.

Kafka assigns partitions to consumers.

### Kafka partitions

Partitions are the unit of parallelism.

More partitions allow more consumers to process messages in parallel.

### Kafka offsets

Offsets identify the position of a message in a partition.

Manual commit gives the application control over when Kafka considers a message processed.

### Dead-letter queue

A DLQ is not a trash bin.

It is an operational signal that something must be inspected.

### Idempotency

Idempotency means the same event can be received more than once without causing repeated downstream effects.

### Testcontainers

Testcontainers allows integration tests to use real infrastructure without depending on manually running services.

---

## 19. Operational Runbook Summary

The full operational guide is available in:

```text
docs/Runbook.md
```

Key operational scenarios include:

```text
Kafka unavailable
MongoDB unavailable
invalid JSON
missing routing rule
empty destination topics
duplicate message
DLQ growth
consumer lag
Worker unhealthy
technical failures
```

Before escalating an issue, collect:

```text
timestamp
Worker instance name
eventId
eventType
correlationId
error code
health output
diagnostics output
metrics output
Worker logs
consumer group lag
DLQ sample
routing rule
```

---

## 20. Production Readiness Summary

The full checklist is available in:

```text
docs/ProductionReadinessChecklist.md
```

The current project is:

```text
GO for local learning
GO for internal technical demo
CONDITIONAL GO for shared development
NO-GO for staging
NO-GO for production
```

Main production gaps:

```text
external observability
secret management
security hardening
diagnostic endpoint protection
Kafka/MongoDB authentication and TLS
CI/CD
deployment manifests
load testing
DLQ reprocessing tooling
retention policies
backup/restore strategy
```

---

## 21. Recommended Future Improvements

Recommended hardening order:

```text
1. Add CI pipeline.
2. Add OpenTelemetry metrics, logs, and traces.
3. Protect diagnostic endpoints.
4. Move secrets to a secret store.
5. Add Kafka and MongoDB authentication/TLS.
6. Add retention policy for processed_messages.
7. Add DLQ replay tooling.
8. Add consumer lag monitoring.
9. Add load tests.
10. Add Kubernetes manifests or target deployment templates.
```

---

## 22. Final Summary

Kafka Router Worker demonstrates how to build a realistic event-driven .NET microservice.

The strongest parts of the project are:

```text
clear separation of concerns
manual Kafka commit
dynamic MongoDB routing
DLQ handling
idempotency
retry handling
health checks
diagnostics
metrics
structured logs
Docker environment
unit tests
integration tests
Testcontainers
operational documentation
architecture documentation
production-readiness thinking
```

The project is not yet production-ready, but it is a strong foundation for understanding how production-oriented messaging systems are designed.

The most important engineering lesson is:

```text
A reliable messaging system is not only about consuming and producing messages.
It is about knowing exactly when to commit, when to retry, when to dead-letter,
how to avoid duplicates, and how to observe the system when something goes wrong.
```