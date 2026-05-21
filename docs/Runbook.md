# Kafka Router Worker - Operational Runbook

Related documents:

- [Course Book](./CourseBook.md)
- [Architecture Review](./ArchitectureReview.md)
- [Production Readiness Checklist](./ProductionReadinessChecklist.md)
- [Final Review](./FinalReview.md)

## 1. Purpose

This runbook explains how to operate and troubleshoot the Kafka Router Worker.

The service consumes events from Kafka, routes them to destination topics based on MongoDB routing rules, stores processed messages for idempotency, and sends invalid or unroutable messages to the dead-letter topic.

## 2. Main Components

| Component | Responsibility |
|---|---|
| Kafka | Provides inbound, destination, and dead-letter topics |
| MongoDB | Stores routing rules and processed message records |
| Kafka Router Worker | Consumes, routes, produces, deduplicates, and handles DLQ |
| Docker Compose | Runs the local environment |

## 3. Main Topics

| Topic | Purpose |
|---|---|
| `events.inbound` | Main input topic |
| `events.crm` | CRM destination topic |
| `events.billing` | Billing destination topic |
| `events.notifications` | Notifications destination topic |
| `events.dead-letter` | Dead-letter topic |

## 4. MongoDB Collections

| Collection | Purpose |
|---|---|
| `routing_rules` | Routing rules by event type |
| `processed_messages` | Processed event IDs for idempotency |

## 5. Operational Endpoints

| Endpoint | Purpose |
|---|---|
| `/health/live` | Checks if the process is alive |
| `/health/ready` | Checks if Kafka and MongoDB are available |
| `/metrics` | Returns in-memory metrics |
| `/diagnostics/config` | Returns sanitized runtime configuration |
| `/diagnostics/routing-rules` | Returns enabled routing rules |
| `/diagnostics/status` | Returns operational status summary |

## 6. Local Environment Commands

Start the environment:

```bash
cd infra
docker compose up --build --scale kafka-router-worker=3
```

Stop the environment:

```bash
cd infra
docker compose down
```

List containers:

```bash
docker compose ps
```

View Worker logs:

```bash
docker logs infra-kafka-router-worker-1 --tail 200
```

Check readiness:

```bash
docker exec infra-kafka-router-worker-1 curl -i http://localhost:8080/health/ready
```

Check diagnostics:

```bash
docker exec infra-kafka-router-worker-1 curl -s http://localhost:8080/diagnostics/status
```

Check metrics:

```bash
docker exec infra-kafka-router-worker-1 curl -s http://localhost:8080/metrics
```

> Container names may vary depending on the Docker Compose project name. Use `docker compose ps` to verify actual names.

## 7. Kafka Commands

List topics:

```bash
docker exec kafka-router-broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

Describe consumer group:

```bash
docker exec kafka-router-broker /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group kafka-router-worker-local
```

Produce a valid test message:

```bash
docker exec -i kafka-router-broker /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic events.inbound <<'EOF'
{"eventId":"manual-test-001","eventType":"CustomerCreated","occurredAt":"2026-05-20T10:00:00Z","source":"manual-test","correlationId":"manual-correlation-001","payload":{"customerId":"CUST-001","email":"test@example.com"}}
EOF
```

Consume dead-letter messages:

```bash
docker exec kafka-router-broker /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic events.dead-letter \
  --from-beginning \
  --timeout-ms 10000
```

## 8. MongoDB Commands

Open Mongo shell:

```bash
docker exec -it kafka-router-mongodb mongosh \
  --username root \
  --password rootpassword \
  --authenticationDatabase admin
```

Select database:

```javascript
use kafka_router
```

View routing rules:

```javascript
db.routing_rules.find().pretty()
```

View processed messages:

```javascript
db.processed_messages.find().pretty()
```

## 9. Failure Scenario: Kafka unavailable

### Symptoms

- `/health/live` returns `200`
- `/health/ready` returns `503`
- `/diagnostics/status` shows Kafka as unhealthy
- Worker logs contain Kafka connection or metadata errors

### Checks

```bash
docker compose ps
docker logs kafka-router-broker --tail 200
docker exec infra-kafka-router-worker-1 curl -i http://localhost:8080/health/ready
docker exec infra-kafka-router-worker-1 curl -s http://localhost:8080/diagnostics/status
```

### Corrective actions

1. Verify Kafka container is running.
2. Verify Kafka bootstrap server configuration.
3. Verify required topics exist.
4. Restart the local environment if Kafka failed during startup.

## 10. Failure Scenario: MongoDB unavailable

### Symptoms

- `/health/live` returns `200`
- `/health/ready` returns `503`
- `/diagnostics/status` shows MongoDB as unhealthy
- Worker logs contain MongoDB timeout or connection errors

### Checks

```bash
docker compose ps
docker logs kafka-router-mongodb --tail 200
docker exec infra-kafka-router-worker-1 curl -i http://localhost:8080/health/ready
docker exec infra-kafka-router-worker-1 curl -s http://localhost:8080/diagnostics/status
```

### Corrective actions

1. Verify MongoDB container is running.
2. Verify credentials and connection string.
3. Verify database and collection names.
4. Restart MongoDB if needed.

## 11. Failure Scenario: Invalid JSON

### Symptoms

- Message goes to `events.dead-letter`
- Metrics show `DeadLetterMessages > 0`
- DLQ error code is `INVALID_JSON`

### Corrective actions

1. Inspect the DLQ message.
2. Check `errorCode`.
3. Check `originalPayload`.
4. Fix the upstream producer payload.

## 12. Failure Scenario: Missing routing rule

### Symptoms

- Valid message goes to DLQ
- DLQ error code is `ROUTING_RULE_NOT_FOUND`

### Checks

```bash
docker exec infra-kafka-router-worker-1 curl -s http://localhost:8080/diagnostics/routing-rules
```

MongoDB:

```javascript
db.routing_rules.find({ eventType: "YourEventType" }).pretty()
```

### Corrective actions

1. Verify the message `eventType`.
2. Add or enable the missing routing rule.
3. Reprocess only after fixing the root cause.

## 13. Failure Scenario: Duplicate message

### Symptoms

- Message is consumed but not routed again
- Metrics show `DuplicateMessages > 0`
- `processed_messages` already contains the `eventId`

### Checks

```javascript
db.processed_messages.find({ eventId: "YourEventId" }).pretty()
```

### Expected behavior

Duplicates are expected in distributed systems. The Worker must skip duplicate routing and commit the offset.

## 14. Failure Scenario: DLQ growing

### Symptoms

- `DeadLetterMessages` keeps increasing
- `events.dead-letter` contains many messages

### Probable causes

- Invalid producer payloads
- Missing routing rules
- Empty destination topics
- Contract change between producer and Worker

### Corrective actions

1. Inspect DLQ samples.
2. Group failures by `errorCode`.
3. Fix producer contract or routing configuration.
4. Reprocess carefully in small batches.

## 15. Application Failure vs Technical Failure

| Type | Example | Retry? | Destination |
|---|---|---:|---|
| Application failure | Invalid JSON | No | DLQ |
| Application failure | Missing routing rule | No | DLQ |
| Application failure | Empty destination topics | No | DLQ |
| Duplicate message | Same `eventId` already processed | No | Skip and commit |
| Technical failure | Kafka unavailable | Yes | Retry |
| Technical failure | MongoDB timeout | Yes | Retry |

## 16. Escalation Checklist

Before escalating, collect:

```text
- Timestamp
- Worker instance name
- EventId
- EventType
- CorrelationId
- Error code
- /health/ready output
- /diagnostics/status output
- /metrics output
- Worker logs
- Kafka consumer group lag
- DLQ sample message
- MongoDB routing rule
```

## 17. Safe Reprocessing Guidelines

Do not blindly replay all DLQ messages.

Before reprocessing:

1. Identify the failure reason.
2. Fix the root cause.
3. Select a small sample.
4. Reprocess in batches.
5. Monitor metrics, DLQ, and consumer lag.
6. Stop immediately if DLQ grows again.

## 18. Known Local Caveats

### Docker Compose container names

Container names may vary depending on the Docker Compose project name.

Use:

```bash
docker compose ps
```

to verify actual container names.

### Testcontainers Kafka image on Apple Silicon

On Apple Silicon, `confluentinc/confluent-local:7.5.0` may fail with a KRaft metadata error.

For this project, Kafka integration tests use:

```text
apache/kafka-native:3.8.0
```

### Testcontainers vulnerability warning

The integration test project may show a warning related to a transitive `SharpCompress` package dependency.

This comes from the test tooling dependency chain and should be reviewed during dependency hardening.