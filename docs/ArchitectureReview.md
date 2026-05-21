# Kafka Router Worker - Architecture Review

Related documents:

- [Course Book](./CourseBook.md)
- [Operational Runbook](./Runbook.md)
- [Production Readiness Checklist](./ProductionReadinessChecklist.md)
- [Final Review](./FinalReview.md)

## 1. Purpose

Kafka Router Worker is a horizontally scalable .NET Worker service that consumes events from a Kafka inbound topic, determines destination topics through MongoDB routing rules, produces routed messages to Kafka, and stores processed messages to support idempotency.

The service is designed around a simple but important principle:

> consume once, route safely, avoid duplicate downstream effects, and make failures observable.

## 2. High-Level Architecture

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
          reads rules│       │stores processed ids
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

## 3. Main Runtime Flow

The normal processing flow is:

```text
1. Worker consumes a message from Kafka input topic.
2. Message payload is parsed into an EventEnvelope.
3. Worker checks whether the EventId was already processed.
4. Worker loads the enabled routing rule for the EventType from MongoDB.
5. Worker produces the original message to all configured destination topics.
6. Worker stores the EventId in processed_messages.
7. Worker commits the Kafka offset.
8. Metrics and structured logs are updated.
```

This flow intentionally separates:

| Concern | Component |
|---|---|
| Kafka consumption | `KafkaMessageConsumer` |
| Kafka production | `KafkaMessageProducer` |
| Parsing | `EventEnvelopeParser` |
| Routing decision | `MongoDbEventRoutingService` |
| DLQ payload creation | `DeadLetterMessageFactory` |
| Idempotency persistence | `ProcessedMessageRepository` |
| Routing rules persistence | `RoutingRuleRepository` |
| Processing orchestration | `MessageProcessingService` |
| Retry policy | `MessageProcessingRetryService` |
| Runtime loop | `Worker` |

## 4. Core Design Decisions

### 4.1 Manual Kafka Commit

The service uses manual offset commit.

This is important because the Worker should commit an offset only after it has completed the processing decision.

The offset is committed after:

| Scenario | Offset committed? |
|---|---:|
| Message successfully routed | Yes |
| Message sent to DLQ due to application failure | Yes |
| Message detected as duplicate | Yes |
| Technical failure before safe completion | No |

This design avoids losing messages during technical failures.

### 4.2 Application Failures Go to DLQ

Application failures are failures caused by the message content or business configuration.

Examples:

| Failure | Error Code |
|---|---|
| Invalid JSON | `INVALID_JSON` |
| Missing `eventId` | `MISSING_EVENT_ID` |
| Missing `eventType` | `MISSING_EVENT_TYPE` |
| Missing routing rule | `ROUTING_RULE_NOT_FOUND` |
| Empty destination topics | `EMPTY_DESTINATION_TOPICS` |

These failures are not retried because retrying the same invalid message would not fix the problem.

Instead, the message is sent to the dead-letter topic and the offset is committed.

### 4.3 Technical Failures Are Retried

Technical failures are infrastructure or unexpected runtime failures.

Examples:

| Failure |
|---|
| Temporary Kafka produce failure |
| MongoDB timeout |
| Network instability |
| Unexpected exception |

These failures are retried through `MessageProcessingRetryService` using Polly.

The key distinction is:

```text
application failure = message/configuration problem
technical failure   = infrastructure/runtime problem
```

### 4.4 Idempotency Through MongoDB

The service stores processed `eventId` values in MongoDB.

This prevents the same event from being routed more than once.

The important collection is:

```text
processed_messages
```

The expected behavior is:

```text
first event with eventId X
  -> route to destination topics
  -> insert X into processed_messages

second event with eventId X
  -> detect duplicate
  -> do not route again
  -> commit offset
```

This protects downstream systems from duplicate side effects.

### 4.5 Dynamic Routing Rules

Routing is data-driven through MongoDB.

This means destination topics can be changed without recompiling the Worker.

Example rule:

```json
{
  "eventType": "CustomerCreated",
  "destinationTopics": ["events.crm"],
  "isEnabled": true
}
```

This gives flexibility, but also introduces an operational responsibility: routing rules must be maintained carefully.

### 4.6 Horizontal Scalability

The Worker can run in multiple instances using the same Kafka consumer group.

Kafka distributes partitions among instances.

Important principle:

```text
maximum useful parallelism is limited by the number of partitions in the input topic
```

If `events.inbound` has one partition, scaling to three Worker instances does not triple throughput.

### 4.7 Observability Built In

The service exposes:

| Endpoint | Purpose |
|---|---|
| `/health/live` | Process liveness |
| `/health/ready` | Dependency readiness |
| `/metrics` | In-memory processing counters |
| `/diagnostics/config` | Sanitized runtime configuration |
| `/diagnostics/routing-rules` | Enabled routing rules |
| `/diagnostics/status` | Operational summary |

This gives a basic but useful observability layer for local and operational troubleshooting.

## 5. Component Review

## 5.1 Worker

`Worker` owns the long-running loop.

Its responsibilities are intentionally limited:

```text
subscribe to Kafka
consume messages
call the retry service
handle shutdown
log lifecycle information
```

It does not contain parsing, routing, DLQ, MongoDB, or Kafka production logic.

This is a good separation because the Worker class remains a runtime coordinator, not a business logic container.

## 5.2 MessageProcessingService

`MessageProcessingService` is the heart of the application.

It coordinates:

```text
parsing
idempotency check
routing
destination production
DLQ production
processed message insert
offset commit
metrics update
structured logs
```

This class is central, but still testable because its dependencies are injected.

It has both unit tests and end-to-end-style integration tests.

## 5.3 MessageProcessingRetryService

`MessageProcessingRetryService` separates retry behavior from processing behavior.

This is important because retry logic can become noisy and hard to reason about if mixed directly into the processing flow.

Current retry behavior is intentionally simple:

```text
retry technical exceptions
do not retry application failures
track consecutive technical failures
update technical failure metrics
```

Future improvement: classify technical exceptions more precisely instead of treating every exception as technical.

## 5.4 KafkaMessageConsumer

`KafkaMessageConsumer` wraps the Confluent.Kafka consumer.

Key responsibilities:

```text
subscribe to input topic
consume messages
commit offsets
close/dispose consumer cleanly
```

The service uses manual commit to preserve control over offset progression.

## 5.5 KafkaMessageProducer

`KafkaMessageProducer` wraps Kafka production.

Important settings:

```text
Acks = All
EnableIdempotence = true
```

These settings improve delivery reliability at the Kafka producer level.

This does not replace application-level idempotency, because producer idempotence and business event idempotency solve different problems.

## 5.6 MongoDbEventRoutingService

This service maps an `EventEnvelope` to a routing decision.

It handles:

```text
missing event type
missing rule
disabled/non-existing rule
empty destination topics
valid route
```

This keeps routing logic separate from the processing orchestration.

## 5.7 RoutingRuleRepository

This repository manages routing rules.

Responsibilities:

```text
create indexes
seed default rules
load enabled rules
load enabled rule by event type
```

It has real MongoDB integration tests through Testcontainers.

## 5.8 ProcessedMessageRepository

This repository supports idempotency.

Responsibilities:

```text
create indexes
check whether eventId already exists
try insert processed message
return false on duplicate
```

The duplicate handling is a critical reliability feature and is tested against real MongoDB.

## 5.9 DeadLetterMessageFactory

This component creates the DLQ payload.

The DLQ message includes:

```text
original Kafka topic
partition
offset
key
error code
error message
failed timestamp
eventId if available
eventType if available
correlationId if available
original payload
```

This is important because a DLQ without diagnostic context is very hard to operate.

## 5.10 Metrics

Metrics are currently in-memory.

They track:

```text
processed messages
dead-letter messages
duplicate messages
technical failures
last processed event
last DLQ event
last duplicate event
processing durations
average processing duration
max processing duration
```

This is useful locally and for learning.

Production improvement: export metrics to a real monitoring system such as OpenTelemetry/Prometheus/Application Insights.

## 5.11 Health Checks

Health checks distinguish between:

```text
liveness = process is alive
readiness = process can safely work
```

This distinction is important.

A Worker can be alive but not ready if Kafka or MongoDB are unavailable.

## 5.12 Diagnostic Endpoints

Diagnostic endpoints provide operator-facing information.

They are not meant to replace logs or monitoring, but they help quickly understand runtime state.

Important security note:

```text
/diagnostics/config must never expose secrets
```

The MongoDB connection string is sanitized.

## 6. Testing Strategy Review

The project now has several layers of testing.

## 6.1 Unit Tests

Unit tests cover isolated logic such as:

```text
parsing
routing decisions
message processing branches
retry behavior
metrics
options validation
connection string sanitization
```

These tests are fast and should run frequently.

## 6.2 HTTP Integration Tests

HTTP integration tests use `WebApplicationFactory`.

They verify:

```text
health endpoints
metrics endpoint
diagnostic endpoints
application startup wiring
```

External dependencies are mocked in this layer.

## 6.3 MongoDB Integration Tests

MongoDB integration tests use Testcontainers.

They verify repositories against a real MongoDB instance.

This catches problems that mocks cannot catch, such as:

```text
wrong collection names
wrong indexes
duplicate key behavior
serialization issues
query behavior
```

## 6.4 Kafka Integration Tests

Kafka integration tests use Testcontainers with:

```text
apache/kafka-native:3.8.0
```

This was selected because it works reliably on Apple Silicon in this project.

The Kafka smoke test verifies:

```text
topic creation
message production
message consumption
```

## 6.5 Controlled End-to-End Test

The controlled E2E test uses:

```text
real Kafka
real MongoDB
real consumer
real producer
real parser
real routing service
real processing service
real metrics
```

It does not start the full background Worker loop.

This is a deliberate trade-off: it gives high confidence while avoiding fragile timing issues around hosted service lifetime and infinite consume loops.

## 7. Strengths of the Current Architecture

The current architecture has several strong points.

### 7.1 Clear Separation of Responsibilities

Each major behavior has a dedicated component.

This makes the code easier to test, explain, and change.

### 7.2 Explicit Failure Handling

The project distinguishes between:

```text
application failures
technical failures
duplicates
successful processing
```

This is much better than treating every failure as a generic exception.

### 7.3 Good Local Operability

The service can be run locally with:

```text
Kafka
MongoDB
Mongo Express
multiple Worker instances
health checks
diagnostic endpoints
metrics
```

This makes the project realistic and useful for learning.

### 7.4 Real Integration Testing

The project does not rely only on mocks.

It verifies important behavior with real infrastructure through Testcontainers.

### 7.5 Idempotency

The system explicitly protects against duplicate event processing.

This is essential in distributed messaging systems.

### 7.6 Configurable Routing

Routing rules are externalized to MongoDB.

This allows behavior changes without code deployment.

## 8. Current Limitations

The project is strong for a learning system, but it is not yet production-complete.

## 8.1 In-Memory Metrics

Metrics are lost when the process restarts.

Production-grade systems should export metrics externally.

Possible improvements:

```text
OpenTelemetry
Prometheus
Grafana
Application Insights
Azure Monitor
```

## 8.2 No Distributed Tracing

The service has correlation IDs in logs, but not full distributed traces.

A production system should propagate and export traces.

## 8.3 No Centralized Logging

Logs are currently local/container logs.

Production should send logs to a centralized platform.

Examples:

```text
ELK
Grafana Loki
Azure Application Insights
Splunk
Datadog
```

## 8.4 No Authentication on Diagnostic Endpoints

Diagnostic endpoints expose operational data.

They are safe enough for local development, but in production they should be protected or restricted.

## 8.5 Simple Retry Classification

Currently technical exception classification is broad.

Future improvement:

```text
retry only transient exceptions
do not retry non-transient configuration errors
define exception categories
```

## 8.6 No DLQ Reprocessing Tool

The runbook describes safe reprocessing principles, but the project does not yet include a DLQ replay utility.

Future improvement:

```text
build a controlled DLQ reprocessor
support filtering by errorCode/eventType/date
support dry-run mode
support batch size limits
```

## 8.7 No Schema Registry

The event envelope is validated manually.

A production event-driven architecture may use:

```text
JSON Schema
Avro
Protobuf
Confluent Schema Registry
contract testing
```

## 8.8 No Secret Management

Local configuration uses plain connection strings.

Production should use a secret store.

Examples:

```text
Azure Key Vault
Kubernetes secrets
Docker secrets
environment-specific secret injection
```

## 8.9 No Kubernetes Manifests

The service is containerized, but not packaged for Kubernetes.

Production deployment would need:

```text
Deployment
Service
ConfigMap
Secret
readiness probe
liveness probe
resource limits
horizontal scaling configuration
```

## 8.10 Limited Performance Testing

The project does not yet measure throughput under load.

Future improvement:

```text
load test with many Kafka messages
measure consumer lag
measure MongoDB latency
measure processing duration distribution
test scaling with multiple partitions
```

## 9. Important Trade-Offs

## 9.1 MongoDB for Idempotency

Using MongoDB for idempotency is simple and effective.

Trade-off:

```text
benefit: easy to implement and query
cost: every new event requires a MongoDB check and insert
```

For high throughput, this could become a bottleneck.

## 9.2 Dynamic Routing Rules

Dynamic routing gives flexibility.

Trade-off:

```text
benefit: change routing without deployment
cost: misconfigured data can break routing
```

This is why diagnostics and runbook matter.

## 9.3 Controlled E2E Instead of Full Worker E2E

The project tests the processing pipeline with real dependencies but does not start the full infinite Worker loop in E2E tests.

Trade-off:

```text
benefit: stable and deterministic tests
cost: slightly less coverage of hosted service runtime behavior
```

This is a good trade-off at this stage.

## 9.4 In-Memory Metrics

In-memory metrics are easy to implement and useful for learning.

Trade-off:

```text
benefit: simple and dependency-free
cost: not durable and not centralized
```

## 10. Production Readiness Assessment

Current status:

| Area | Status |
|---|---|
| Core processing logic | Strong |
| Failure handling | Good |
| Idempotency | Good |
| Local Docker environment | Good |
| Unit testing | Good |
| Integration testing | Good |
| E2E confidence | Good for learning/local |
| Observability | Basic |
| Security | Local-only |
| Deployment automation | Basic/local |
| Production monitoring | Missing |
| Secret management | Missing |
| DLQ operations | Documented, not automated |
| Performance validation | Missing |

Overall assessment:

```text
The project is a strong learning-grade and local integration-grade system.
It demonstrates many production-oriented patterns, but it still needs
external observability, security hardening, deployment manifests,
secret management, and performance testing before real production use.
```

## 11. Recommended Future Improvements

Recommended improvements in priority order:

```text
1. Add OpenTelemetry metrics and traces.
2. Export logs and metrics to an external platform.
3. Protect diagnostic endpoints.
4. Add a DLQ replay/reprocessing utility.
5. Add schema validation or schema registry integration.
6. Add Kubernetes manifests.
7. Add secret management.
8. Add load tests.
9. Add consumer lag monitoring.
10. Add CI pipeline running unit and integration tests.
```

## 12. Final Architectural Summary

Kafka Router Worker is designed around a clean processing pipeline:

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

The architecture is intentionally modular.

The strongest parts are:

```text
clear separation of concerns
explicit failure handling
idempotency
real integration testing
local operability
diagnostic endpoints
```

The main missing production-grade areas are:

```text
external observability
security
secret management
deployment hardening
performance validation
DLQ automation
```

The project is therefore an excellent foundation for understanding how a real event-driven .NET microservice can be designed, tested, operated, and evolved.