using FluentAssertions;
using KafkaRouter.Worker.IntegrationTests.EndToEnd.Infrastructure;
using KafkaRouter.Worker.IntegrationTests.Kafka.Infrastructure;

namespace KafkaRouter.Worker.IntegrationTests.EndToEnd;

public sealed class MessageProcessingEndToEndTests
    : IClassFixture<KafkaMongoDbTestcontainerFixture>
{
    private readonly KafkaMongoDbTestcontainerFixture _fixture;

    public MessageProcessingEndToEndTests(
        KafkaMongoDbTestcontainerFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task ProcessAsync_WhenMessageIsValid_ShouldRouteToDestinationTopicAndSaveProcessedMessage()
    {
        // Arrange
        var uniqueSuffix = Guid.NewGuid().ToString("N");

        var inputTopic = $"events-inbound-{uniqueSuffix}";
        var deadLetterTopic = $"events-dead-letter-{uniqueSuffix}";
        var destinationTopic = "events.crm";
        var consumerGroupId = $"e2e-group-{uniqueSuffix}";

        await KafkaTestClient.CreateTopicAsync(
            _fixture.KafkaBootstrapServers,
            inputTopic);

        await KafkaTestClient.CreateTopicAsync(
            _fixture.KafkaBootstrapServers,
            deadLetterTopic);

        await KafkaTestClient.CreateTopicAsync(
            _fixture.KafkaBootstrapServers,
            destinationTopic);

        var kafkaOptions = EndToEndComponentFactory.CreateKafkaOptions(
            _fixture,
            inputTopic,
            deadLetterTopic,
            consumerGroupId);

        var workerOptions = EndToEndComponentFactory.CreateWorkerOptions();

        var routingRuleRepository = EndToEndComponentFactory.CreateRoutingRuleRepository(
            _fixture);

        var processedMessageRepository = EndToEndComponentFactory.CreateProcessedMessageRepository(
            _fixture);

        await routingRuleRepository.EnsureIndexesAsync(CancellationToken.None);
        await routingRuleRepository.SeedDefaultRulesAsync(CancellationToken.None);

        await processedMessageRepository.EnsureIndexesAsync(CancellationToken.None);

        var workerMetrics = EndToEndComponentFactory.CreateWorkerMetrics(
            workerOptions);

        using var kafkaConsumer = EndToEndComponentFactory.CreateKafkaMessageConsumer(
            kafkaOptions,
            workerOptions);

        using var kafkaProducer = EndToEndComponentFactory.CreateKafkaMessageProducer(
            kafkaOptions,
            workerOptions);

        var messageProcessingService = EndToEndComponentFactory.CreateMessageProcessingService(
            kafkaOptions,
            workerOptions,
            kafkaConsumer,
            kafkaProducer,
            routingRuleRepository,
            processedMessageRepository,
            workerMetrics);

        kafkaConsumer.Subscribe();

        var eventId = $"event-e2e-{uniqueSuffix}";

        var inputPayload =
            $$"""
            {
              "eventId": "{{eventId}}",
              "eventType": "CustomerCreated",
              "occurredAt": "2026-05-20T10:00:00Z",
              "source": "integration-test",
              "correlationId": "correlation-{{uniqueSuffix}}",
              "payload": {
                "customerId": "CUST-E2E",
                "email": "e2e@example.com"
              }
            }
            """;

        await KafkaTestClient.ProduceAsync(
            _fixture.KafkaBootstrapServers,
            inputTopic,
            key: eventId,
            value: inputPayload);

        var consumeResult = kafkaConsumer.Consume(
            CancellationToken.None);

        // Act
        var processingResult = await messageProcessingService.ProcessAsync(
            consumeResult,
            CancellationToken.None);

        // Assert
        processingResult.IsSuccessful.Should().BeTrue();
        processingResult.EventId.Should().Be(eventId);
        processingResult.EventType.Should().Be("CustomerCreated");
        processingResult.ProcessingDurationMs.Should().BeGreaterThanOrEqualTo(0);

        var exists = await processedMessageRepository.ExistsByEventIdAsync(
            eventId,
            CancellationToken.None);

        exists.Should().BeTrue();

        var routedPayload = KafkaTestClient.ConsumeSingleValue(
            _fixture.KafkaBootstrapServers,
            destinationTopic,
            groupId: $"destination-checker-{uniqueSuffix}",
            timeout: TimeSpan.FromSeconds(15));

        routedPayload.Should().Be(inputPayload);

        var metricsSnapshot = workerMetrics.GetSnapshot();

        metricsSnapshot.ProcessedMessages.Should().Be(1);
        metricsSnapshot.DeadLetterMessages.Should().Be(0);
        metricsSnapshot.DuplicateMessages.Should().Be(0);
        metricsSnapshot.LastProcessedEventId.Should().Be(eventId);
        metricsSnapshot.LastProcessedEventType.Should().Be("CustomerCreated");
        metricsSnapshot.LastProcessedDurationMs.Should().NotBeNull();
    }
}