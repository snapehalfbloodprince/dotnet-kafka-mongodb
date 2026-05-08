using Confluent.Kafka;
using FluentAssertions;
using KafkaRouter.Worker.DeadLetter;
using KafkaRouter.Worker.Kafka;
using KafkaRouter.Worker.Metrics;
using KafkaRouter.Worker.MongoDb.Documents;
using KafkaRouter.Worker.MongoDb.Repositories;
using KafkaRouter.Worker.Options;
using KafkaRouter.Worker.Parsing;
using KafkaRouter.Worker.Processing;
using KafkaRouter.Worker.Routing;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using System.Text.Json;

namespace KafkaRouter.Worker.UnitTests.Processing;

public sealed class MessageProcessingServiceTests
{
    private readonly Mock<IKafkaMessageConsumer> _kafkaMessageConsumerMock = new();
    private readonly Mock<IKafkaMessageProducer> _kafkaMessageProducerMock = new();
    private readonly Mock<IEventRoutingService> _eventRoutingServiceMock = new();
    private readonly Mock<IProcessedMessageRepository> _processedMessageRepositoryMock = new();
    private readonly Mock<IWorkerMetrics> _workerMetricsMock = new();

    private readonly IEventEnvelopeParser _eventEnvelopeParser = new EventEnvelopeParser();
    private readonly IDeadLetterMessageFactory _deadLetterMessageFactory = new DeadLetterMessageFactory();

    [Fact]
    public async Task ProcessAsync_WhenMessageIsValid_ShouldProduceToDestinationTopicsSaveProcessedMessageCommitAndReturnSuccess()
    {
        // Arrange
        var consumeResult = CreateConsumeResult(
            topic: "events.inbound",
            partition: 0,
            offset: 10,
            key: "event-valid-001",
            value: """
            {
              "eventId": "event-valid-001",
              "eventType": "CustomerCreated",
              "occurredAt": "2026-05-08T10:00:00Z",
              "source": "unit-test",
              "correlationId": "correlation-valid-001",
              "payload": {
                "customerId": "CUST-001"
              }
            }
            """);

        _processedMessageRepositoryMock
            .Setup(repository => repository.ExistsByEventIdAsync(
                "event-valid-001",
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        _eventRoutingServiceMock
            .Setup(service => service.GetRoutingDecisionAsync(
                It.IsAny<KafkaRouter.Worker.Models.EventEnvelope>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(RoutingDecision.RouteTo(
                "events.crm",
                "events.notifications"));

        _processedMessageRepositoryMock
            .Setup(repository => repository.TryInsertAsync(
                It.IsAny<ProcessedMessageDocument>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        _kafkaMessageProducerMock
            .Setup(producer => producer.ProduceAsync(
                It.IsAny<string>(),
                It.IsAny<string?>(),
                It.IsAny<string>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(CreateDeliveryResult("events.crm"));

        var sut = CreateSut();

        // Act
        var result = await sut.ProcessAsync(
            consumeResult,
            CancellationToken.None);

        // Assert
        result.Outcome.Should().Be(MessageProcessingOutcome.ProcessedSuccessfully);
        result.EventId.Should().Be("event-valid-001");
        result.EventType.Should().Be("CustomerCreated");
        result.CorrelationId.Should().Be("correlation-valid-001");

        _kafkaMessageProducerMock.Verify(
            producer => producer.ProduceAsync(
                "events.crm",
                "event-valid-001",
                consumeResult.Message.Value,
                It.IsAny<CancellationToken>()),
            Times.Once);

        _kafkaMessageProducerMock.Verify(
            producer => producer.ProduceAsync(
                "events.notifications",
                "event-valid-001",
                consumeResult.Message.Value,
                It.IsAny<CancellationToken>()),
            Times.Once);

        _processedMessageRepositoryMock.Verify(
            repository => repository.TryInsertAsync(
                It.Is<ProcessedMessageDocument>(document =>
                    document.EventId == "event-valid-001"
                    && document.EventType == "CustomerCreated"
                    && document.SourceTopic == "events.inbound"
                    && document.SourcePartition == 0
                    && document.SourceOffset == 10
                    && document.DestinationTopics.Contains("events.crm")
                    && document.DestinationTopics.Contains("events.notifications")),
                It.IsAny<CancellationToken>()),
            Times.Once);

        _workerMetricsMock.Verify(
            metrics => metrics.IncrementProcessedMessages(
                "event-valid-001",
                "CustomerCreated"),
            Times.Once);

        _kafkaMessageConsumerMock.Verify(
            consumer => consumer.Commit(consumeResult),
            Times.Once);
    }

    [Fact]
    public async Task ProcessAsync_WhenMessageIsInvalidJson_ShouldProduceToDeadLetterCommitAndReturnDeadLetterResult()
    {
        // Arrange
        var consumeResult = CreateConsumeResult(
            topic: "events.inbound",
            partition: 1,
            offset: 20,
            key: null,
            value: "questo non è json");

        _kafkaMessageProducerMock
            .Setup(producer => producer.ProduceAsync(
                It.IsAny<string>(),
                It.IsAny<string?>(),
                It.IsAny<string>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(CreateDeliveryResult("events.dead-letter"));

        var sut = CreateSut();

        // Act
        var result = await sut.ProcessAsync(
            consumeResult,
            CancellationToken.None);

        // Assert
        result.Outcome.Should().Be(MessageProcessingOutcome.SentToDeadLetter);
        result.EventId.Should().BeNull();
        result.EventType.Should().BeNull();
        result.ErrorCode.Should().Be("INVALID_JSON");
        result.CorrelationId.Should().Be("events.inbound-1-20");

        _kafkaMessageProducerMock.Verify(
            producer => producer.ProduceAsync(
                "events.dead-letter",
                "events.inbound-1-20",
                It.Is<string>(payload => IsInvalidJsonDeadLetterPayload(payload)),
                It.IsAny<CancellationToken>()),
            Times.Once);

        _workerMetricsMock.Verify(
            metrics => metrics.IncrementDeadLetterMessages(
                null,
                null,
                "INVALID_JSON"),
            Times.Once);

        _kafkaMessageConsumerMock.Verify(
            consumer => consumer.Commit(consumeResult),
            Times.Once);

        _eventRoutingServiceMock.Verify(
            service => service.GetRoutingDecisionAsync(
                It.IsAny<KafkaRouter.Worker.Models.EventEnvelope>(),
                It.IsAny<CancellationToken>()),
            Times.Never);

        _processedMessageRepositoryMock.Verify(
            repository => repository.TryInsertAsync(
                It.IsAny<ProcessedMessageDocument>(),
                It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task ProcessAsync_WhenRoutingRuleIsNotFound_ShouldProduceToDeadLetterCommitAndReturnDeadLetterResult()
    {
        // Arrange
        var consumeResult = CreateConsumeResult(
            topic: "events.inbound",
            partition: 2,
            offset: 30,
            key: "event-routing-001",
            value: """
            {
              "eventId": "event-routing-001",
              "eventType": "UnknownEvent",
              "occurredAt": "2026-05-08T10:00:00Z",
              "source": "unit-test",
              "correlationId": "correlation-routing-001",
              "payload": {
                "value": "test"
              }
            }
            """);

        _processedMessageRepositoryMock
            .Setup(repository => repository.ExistsByEventIdAsync(
                "event-routing-001",
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        _eventRoutingServiceMock
            .Setup(service => service.GetRoutingDecisionAsync(
                It.IsAny<KafkaRouter.Worker.Models.EventEnvelope>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(RoutingDecision.DeadLetter(
                "ROUTING_RULE_NOT_FOUND",
                "Nessuna routing rule trovata."));

        _kafkaMessageProducerMock
            .Setup(producer => producer.ProduceAsync(
                It.IsAny<string>(),
                It.IsAny<string?>(),
                It.IsAny<string>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(CreateDeliveryResult("events.dead-letter"));

        var sut = CreateSut();

        // Act
        var result = await sut.ProcessAsync(
            consumeResult,
            CancellationToken.None);

        // Assert
        result.Outcome.Should().Be(MessageProcessingOutcome.SentToDeadLetter);
        result.EventId.Should().Be("event-routing-001");
        result.EventType.Should().Be("UnknownEvent");
        result.CorrelationId.Should().Be("correlation-routing-001");
        result.ErrorCode.Should().Be("ROUTING_RULE_NOT_FOUND");

        _kafkaMessageProducerMock.Verify(
            producer => producer.ProduceAsync(
                "events.dead-letter",
                "event-routing-001",
                It.Is<string>(payload =>
                    payload.Contains("ROUTING_RULE_NOT_FOUND")
                    && payload.Contains("event-routing-001")
                    && payload.Contains("UnknownEvent")),
                It.IsAny<CancellationToken>()),
            Times.Once);

        _workerMetricsMock.Verify(
            metrics => metrics.IncrementDeadLetterMessages(
                "event-routing-001",
                "UnknownEvent",
                "ROUTING_RULE_NOT_FOUND"),
            Times.Once);

        _kafkaMessageConsumerMock.Verify(
            consumer => consumer.Commit(consumeResult),
            Times.Once);

        _processedMessageRepositoryMock.Verify(
            repository => repository.TryInsertAsync(
                It.IsAny<ProcessedMessageDocument>(),
                It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task ProcessAsync_WhenMessageIsDuplicate_ShouldNotProduceShouldCommitAndReturnDuplicateResult()
    {
        // Arrange
        var consumeResult = CreateConsumeResult(
            topic: "events.inbound",
            partition: 0,
            offset: 40,
            key: "event-duplicate-001",
            value: """
            {
              "eventId": "event-duplicate-001",
              "eventType": "CustomerCreated",
              "occurredAt": "2026-05-08T10:00:00Z",
              "source": "unit-test",
              "correlationId": "correlation-duplicate-001",
              "payload": {
                "customerId": "CUST-001"
              }
            }
            """);

        _processedMessageRepositoryMock
            .Setup(repository => repository.ExistsByEventIdAsync(
                "event-duplicate-001",
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        var sut = CreateSut();

        // Act
        var result = await sut.ProcessAsync(
            consumeResult,
            CancellationToken.None);

        // Assert
        result.Outcome.Should().Be(MessageProcessingOutcome.SkippedAsDuplicate);
        result.EventId.Should().Be("event-duplicate-001");
        result.EventType.Should().Be("CustomerCreated");
        result.CorrelationId.Should().Be("correlation-duplicate-001");

        _kafkaMessageProducerMock.Verify(
            producer => producer.ProduceAsync(
                It.IsAny<string>(),
                It.IsAny<string?>(),
                It.IsAny<string>(),
                It.IsAny<CancellationToken>()),
            Times.Never);

        _eventRoutingServiceMock.Verify(
            service => service.GetRoutingDecisionAsync(
                It.IsAny<KafkaRouter.Worker.Models.EventEnvelope>(),
                It.IsAny<CancellationToken>()),
            Times.Never);

        _processedMessageRepositoryMock.Verify(
            repository => repository.TryInsertAsync(
                It.IsAny<ProcessedMessageDocument>(),
                It.IsAny<CancellationToken>()),
            Times.Never);

        _workerMetricsMock.Verify(
            metrics => metrics.IncrementDuplicateMessages(
                "event-duplicate-001",
                "CustomerCreated"),
            Times.Once);

        _kafkaMessageConsumerMock.Verify(
            consumer => consumer.Commit(consumeResult),
            Times.Once);
    }

    [Fact]
    public async Task ProcessAsync_WhenProducerThrowsTechnicalException_ShouldPropagateExceptionAndNotCommit()
    {
        // Arrange
        var consumeResult = CreateConsumeResult(
            topic: "events.inbound",
            partition: 0,
            offset: 50,
            key: "event-technical-001",
            value: """
            {
              "eventId": "event-technical-001",
              "eventType": "CustomerCreated",
              "occurredAt": "2026-05-08T10:00:00Z",
              "source": "unit-test",
              "correlationId": "correlation-technical-001",
              "payload": {
                "customerId": "CUST-001"
              }
            }
            """);

        _processedMessageRepositoryMock
            .Setup(repository => repository.ExistsByEventIdAsync(
                "event-technical-001",
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        _eventRoutingServiceMock
            .Setup(service => service.GetRoutingDecisionAsync(
                It.IsAny<KafkaRouter.Worker.Models.EventEnvelope>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(RoutingDecision.RouteTo("events.crm"));

        _kafkaMessageProducerMock
            .Setup(producer => producer.ProduceAsync(
                "events.crm",
                It.IsAny<string?>(),
                It.IsAny<string>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new KafkaException(
                new Error(
                    ErrorCode.Local_MsgTimedOut,
                    "Simulated producer timeout")));

        var sut = CreateSut();

        // Act
        var act = async () => await sut.ProcessAsync(
            consumeResult,
            CancellationToken.None);

        // Assert
        await act.Should().ThrowAsync<KafkaException>();

        _kafkaMessageConsumerMock.Verify(
            consumer => consumer.Commit(It.IsAny<ConsumeResult<string, string>>()),
            Times.Never);

        _processedMessageRepositoryMock.Verify(
            repository => repository.TryInsertAsync(
                It.IsAny<ProcessedMessageDocument>(),
                It.IsAny<CancellationToken>()),
            Times.Never);

        _workerMetricsMock.Verify(
            metrics => metrics.IncrementProcessedMessages(
                It.IsAny<string>(),
                It.IsAny<string>()),
            Times.Never);
    }

    private static bool IsInvalidJsonDeadLetterPayload(string payload)
    {
        using var jsonDocument = JsonDocument.Parse(payload);

        var root = jsonDocument.RootElement;

        return root.GetProperty("errorCode").GetString() == "INVALID_JSON"
            && root.GetProperty("originalPayload").GetString() == "questo non è json"
            && root.GetProperty("originalTopic").GetString() == "events.inbound"
            && root.GetProperty("originalPartition").GetInt32() == 1
            && root.GetProperty("originalOffset").GetInt64() == 20;
    }
    private MessageProcessingService CreateSut()
    {
        var kafkaOptions = Microsoft.Extensions.Options.Options.Create(new KafkaOptions
        {
            BootstrapServers = "localhost:9092",
            InputTopic = "events.inbound",
            DeadLetterTopic = "events.dead-letter",
            ConsumerGroupId = "kafka-router-worker-local",
            AutoOffsetReset = "Earliest"
        });

        return new MessageProcessingService(
            NullLogger<MessageProcessingService>.Instance,
            _kafkaMessageConsumerMock.Object,
            _kafkaMessageProducerMock.Object,
            _eventEnvelopeParser,
            _eventRoutingServiceMock.Object,
            _deadLetterMessageFactory,
            _processedMessageRepositoryMock.Object,
            _workerMetricsMock.Object,
            kafkaOptions);
    }

    private static ConsumeResult<string, string> CreateConsumeResult(
        string topic,
        int partition,
        long offset,
        string? key,
        string value)
    {
        return new ConsumeResult<string, string>
        {
            Topic = topic,
            Partition = new Partition(partition),
            Offset = new Offset(offset),
            Message = new Message<string, string>
            {
                Key = key,
                Value = value
            }
        };
    }

    private static DeliveryResult<string, string> CreateDeliveryResult(string topic)
    {
        return new DeliveryResult<string, string>
        {
            Topic = topic,
            Partition = new Partition(0),
            Offset = new Offset(0),
            Status = PersistenceStatus.Persisted,
            Message = new Message<string, string>
            {
                Key = "test-key",
                Value = "test-value"
            }
        };
    }
}