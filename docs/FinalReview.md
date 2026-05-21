# Kafka Router Worker - Final Review

Related documents:

- [Course Book](./CourseBook.md)
- [Operational Runbook](./Runbook.md)
- [Architecture Review](./ArchitectureReview.md)
- [Production Readiness Checklist](./ProductionReadinessChecklist.md)

---

## 1. Project Summary

Kafka Router Worker is a .NET microservice that consumes events from Kafka, routes them dynamically using MongoDB rules, supports idempotency, handles invalid messages through a dead-letter topic, exposes operational endpoints, and is tested with real infrastructure using Testcontainers.

The project demonstrates how to design a realistic event-driven service with production-oriented patterns.

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

---

## 2. What Was Built

The project includes:

```text
- .NET Worker hosted as ASP.NET Core app
- Kafka consumer
- Kafka producer
- MongoDB routing rules
- MongoDB processed message tracking
- Dynamic routing to multiple Kafka topics
- Dead-letter topic handling
- Application failure classification
- Technical retry with Polly
- Manual Kafka offset commit
- Idempotency
- Structured logging
- CorrelationId propagation
- Health endpoints
- Metrics endpoint
- Diagnostic endpoints
- Docker Compose local environment
- Multi-instance local scaling
- Unit tests
- HTTP integration tests
- MongoDB Testcontainers tests
- Kafka Testcontainers tests
- Controlled end-to-end test
- Operational runbook
- Architecture review
- Production readiness checklist
- Course book documentation
```

---

## 3. Main Technical Concepts Learned

## 3.1 Kafka Topics

Kafka topics are named streams of records.

In this project:

```text
events.inbound
events.crm
events.billing
events.notifications
events.dead-letter
```

The Worker consumes from the inbound topic and produces to destination topics.

## 3.2 Kafka Partitions

Partitions are the unit of Kafka parallelism.

A topic with one partition can only be consumed by one consumer instance in the same group at a time.

A topic with multiple partitions can be processed in parallel by multiple Worker instances.

Key lesson:

```text
Scaling consumers only helps if the topic has enough partitions.
```

## 3.3 Consumer Groups

Multiple Worker instances use the same consumer group:

```text
kafka-router-worker-local
```

Kafka assigns partitions to consumers in the same group.

This allows horizontal scaling while avoiding multiple consumers processing the same partition at the same time.

## 3.4 Kafka Offsets

Offsets represent the position of messages inside a partition.

This project uses manual commit.

The Worker commits offsets only after it reaches a safe outcome:

```text
success
DLQ
duplicate skip
```

It does not commit after unresolved technical failures.

## 3.5 Dead-Letter Queue

The DLQ stores messages that cannot be processed for application reasons.

Examples:

```text
invalid JSON
missing eventId
missing eventType
missing routing rule
empty destination topics
```

A DLQ is not a trash bin. It is an operational signal.

## 3.6 Idempotency

Idempotency prevents duplicate downstream side effects.

The project stores processed event IDs in MongoDB.

If the same `eventId` arrives again, the Worker skips routing and commits the offset.

## 3.7 Retry

Retry is used only for technical failures.

Application failures are not retried because retrying an invalid message does not fix it.

This distinction is central:

```text
application failure = DLQ
technical failure = retry
duplicate = skip
```

## 3.8 Observability

The project exposes:

```text
/health/live
/health/ready
/metrics
/diagnostics/config
/diagnostics/routing-rules
/diagnostics/status
```

This makes the service easier to inspect and operate.

---

## 4. Main Architecture Strengths

## 4.1 Clear Separation of Responsibilities

The architecture separates:

```text
runtime loop
message processing
retry
Kafka consumer
Kafka producer
parsing
routing
DLQ creation
MongoDB repositories
metrics
health
diagnostics
```

This makes the system easier to test and evolve.

## 4.2 Safe Offset Commit Strategy

Manual commit protects against message loss during technical failures.

This is one of the most important design choices in the whole project.

## 4.3 Explicit Failure Outcomes

The system does not treat all errors as generic exceptions.

It distinguishes:

```text
processed successfully
sent to DLQ
skipped as duplicate
technical failure
```

This makes behavior clearer and easier to test.

## 4.4 Real Integration Testing

The project uses Testcontainers for MongoDB and Kafka.

This means important behavior is tested against real infrastructure instead of only mocks.

## 4.5 Operational Documentation

The project includes a runbook, architecture review, production checklist, and course book.

This makes it easier for another developer to understand and operate the system.

---

## 5. Current Project Status

| Area | Status |
|---|---|
| Core processing | Complete |
| Dynamic routing | Complete |
| DLQ handling | Complete |
| Idempotency | Complete |
| Retry | Implemented |
| Health checks | Implemented |
| Metrics | Implemented locally |
| Diagnostics | Implemented locally |
| Docker Compose | Complete for local use |
| Unit tests | Implemented |
| Integration tests | Implemented |
| E2E confidence | Good for local/integration |
| Documentation | Strong |
| Production readiness | Not complete |

Current final assessment:

```text
Excellent learning-grade system.
Strong local integration-grade system.
Good architectural demonstration.
Not production-ready without further hardening.
```

---

## 6. Why It Is Not Yet Production-Ready

The project intentionally stops before full production hardening.

Missing production-grade areas:

```text
- external metrics export
- centralized logging
- distributed tracing
- authentication and authorization
- protected diagnostic endpoints
- secret management
- Kafka TLS/authentication
- MongoDB least-privilege users
- deployment manifests
- CI/CD pipeline
- alerting
- dashboarding
- load testing
- consumer lag monitoring
- DLQ replay tooling
- retention policies
- backup/restore strategy
```

This is normal.

The project teaches the foundation. Production adds governance, security, automation, and operations.

---

## 7. Recommended Next Steps

Recommended next steps in order:

```text
1. Add CI pipeline.
2. Add OpenTelemetry.
3. Export metrics and traces externally.
4. Centralize logs.
5. Protect diagnostic endpoints.
6. Move secrets to a secret store.
7. Add Kafka/MongoDB authentication and TLS.
8. Add processed_messages retention policy.
9. Add consumer lag monitoring.
10. Add DLQ replay tooling.
11. Add load tests.
12. Add Kubernetes manifests or target deployment templates.
```

---

## 8. Suggested Demo Script

This project can be demonstrated effectively in this order.

### 8.1 Explain the problem

```text
We receive events from Kafka and need to route them dynamically to one or more destination topics.
Routing must be configurable, safe, observable, and idempotent.
```

### 8.2 Show the architecture

Show:

```text
CourseBook.md
ArchitectureReview.md
```

Explain:

```text
Kafka input topic
Worker
MongoDB routing rules
destination topics
processed_messages
DLQ
```

### 8.3 Start the environment

```bash
cd infra
docker compose up --build --scale kafka-router-worker=3
```

### 8.4 Show health and diagnostics

```bash
docker exec infra-kafka-router-worker-1 curl -s http://localhost:8080/health/ready
docker exec infra-kafka-router-worker-1 curl -s http://localhost:8080/diagnostics/status
docker exec infra-kafka-router-worker-1 curl -s http://localhost:8080/diagnostics/routing-rules
```

### 8.5 Produce a valid event

```bash
docker exec -i kafka-router-broker /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic events.inbound <<'EOF'
{"eventId":"demo-001","eventType":"CustomerCreated","occurredAt":"2026-05-20T10:00:00Z","source":"demo","correlationId":"demo-correlation-001","payload":{"customerId":"CUST-DEMO","email":"demo@example.com"}}
EOF
```

Then consume from:

```bash
docker exec kafka-router-broker /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic events.crm \
  --from-beginning \
  --timeout-ms 10000
```

### 8.6 Produce an invalid event

```bash
docker exec -i kafka-router-broker /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic events.inbound <<'EOF'
{ invalid json
EOF
```

Then consume from DLQ:

```bash
docker exec kafka-router-broker /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic events.dead-letter \
  --from-beginning \
  --timeout-ms 10000
```

### 8.7 Show duplicate handling

Produce the same event twice and show that `DuplicateMessages` increases.

```bash
docker exec infra-kafka-router-worker-1 curl -s http://localhost:8080/metrics
```

### 8.8 Show tests

```bash
dotnet test
```

Explain that tests include:

```text
unit tests
HTTP integration tests
MongoDB Testcontainers tests
Kafka Testcontainers tests
controlled end-to-end test
```

### 8.9 Close with production checklist

Show:

```text
ProductionReadinessChecklist.md
```

Explain clearly:

```text
The project is strong for learning and demo.
Production would require observability, security, secrets, CI/CD, deployment hardening, and load testing.
```

---

## 9. Interview / Explanation Talking Points

If asked to explain the project, use these points.

### What problem does it solve?

It routes Kafka events dynamically to one or more destination topics based on configuration stored in MongoDB.

### Why MongoDB?

MongoDB is used for dynamic routing rules and processed message tracking. It allows changing routing behavior without recompiling the service.

### Why manual commit?

Manual commit gives the application control over when a message is considered processed.

### Why DLQ?

The DLQ preserves messages that cannot be processed due to application-level problems and makes them inspectable.

### Why idempotency?

Distributed messaging systems can deliver duplicates. Idempotency prevents repeated downstream effects.

### Why Testcontainers?

Testcontainers allows tests to run against real Kafka and MongoDB without requiring manually managed local infrastructure.

### Why not production-ready?

Because production requires external observability, security, secrets, CI/CD, deployment hardening, alerting, and performance validation.

---

## 10. Final Engineering Lessons

The most important lessons from the project are:

```text
- In messaging systems, commit strategy is critical.
- Application failures and technical failures must be treated differently.
- Idempotency is not optional in distributed systems.
- DLQ messages must contain enough context to be useful.
- Scaling consumers requires enough Kafka partitions.
- Observability must be designed from the beginning.
- Integration tests with real dependencies catch issues mocks cannot.
- Documentation is part of the system, not an afterthought.
```

---

## 11. Final Conclusion

Kafka Router Worker is a complete learning project for understanding event-driven microservices with .NET, Kafka, MongoDB, Docker, and Testcontainers.

It demonstrates not only how to write the code, but also how to reason about:

```text
failure
retries
offsets
duplicates
routing
observability
testing
operations
production readiness
```

The final lesson is:

```text
A reliable backend system is not just code that works in the happy path.
It is code that behaves predictably when the unhappy paths happen.
```