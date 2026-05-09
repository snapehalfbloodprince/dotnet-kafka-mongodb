using System.Text.Json;
using Confluent.Kafka;
using FluentAssertions;
using KafkaRouter.Worker.DeadLetter;
using KafkaRouter.Worker.Models;

namespace KafkaRouter.Worker.UnitTests.DeadLetter;

public sealed class DeadLetterMessageFactoryTests
{
    private readonly DeadLetterMessageFactory _sut = new();

    [Fact]
    public void CreateDeadLetterPayload_WhenEventEnvelopeIsProvided_ShouldCreateExpectedJsonPayload()
    {
        // Arrange
        var consumeResult = CreateConsumeResult(
            topic: "events.inbound",
            partition: 1,
            offset: 42,
            key: "event-001",
            value: """
            {
              "eventId": "event-001",
              "eventType": "UnknownEvent"
            }
            """);

        var eventEnvelope = new EventEnvelope
        {
            EventId = "event-001",
            EventType = "UnknownEvent",
            CorrelationId = "correlation-001"
        };

        // Act
        var payload = _sut.CreateDeadLetterPayload(
            consumeResult,
            errorCode: "ROUTING_RULE_NOT_FOUND",
            errorMessage: "No routing rule found.",
            eventEnvelope);

        // Assert
        using var jsonDocument = JsonDocument.Parse(payload);

        var root = jsonDocument.RootElement;

        root.GetProperty("originalTopic").GetString().Should().Be("events.inbound");
        root.GetProperty("originalPartition").GetInt32().Should().Be(1);
        root.GetProperty("originalOffset").GetInt64().Should().Be(42);
        root.GetProperty("originalKey").GetString().Should().Be("event-001");

        root.GetProperty("errorCode").GetString().Should().Be("ROUTING_RULE_NOT_FOUND");
        root.GetProperty("errorMessage").GetString().Should().Be("No routing rule found.");

        root.GetProperty("eventId").GetString().Should().Be("event-001");
        root.GetProperty("eventType").GetString().Should().Be("UnknownEvent");
        root.GetProperty("correlationId").GetString().Should().Be("correlation-001");

        root.GetProperty("originalPayload").GetString().Should().Contain("UnknownEvent");
        root.GetProperty("failedAt").GetDateTimeOffset().Should().BeCloseTo(
            DateTimeOffset.UtcNow,
            TimeSpan.FromSeconds(10));
    }

    [Fact]
    public void CreateDeadLetterPayload_WhenEventEnvelopeIsNull_ShouldCreatePayloadWithNullEventFields()
    {
        // Arrange
        var consumeResult = CreateConsumeResult(
            topic: "events.inbound",
            partition: 0,
            offset: 10,
            key: null,
            value: "invalid json");

        // Act
        var payload = _sut.CreateDeadLetterPayload(
            consumeResult,
            errorCode: "INVALID_JSON",
            errorMessage: "Invalid JSON.",
            eventEnvelope: null);

        // Assert
        using var jsonDocument = JsonDocument.Parse(payload);

        var root = jsonDocument.RootElement;

        root.GetProperty("originalTopic").GetString().Should().Be("events.inbound");
        root.GetProperty("originalPartition").GetInt32().Should().Be(0);
        root.GetProperty("originalOffset").GetInt64().Should().Be(10);

        root.GetProperty("originalKey").ValueKind.Should().Be(JsonValueKind.Null);
        root.GetProperty("eventId").ValueKind.Should().Be(JsonValueKind.Null);
        root.GetProperty("eventType").ValueKind.Should().Be(JsonValueKind.Null);
        root.GetProperty("correlationId").ValueKind.Should().Be(JsonValueKind.Null);

        root.GetProperty("errorCode").GetString().Should().Be("INVALID_JSON");
        root.GetProperty("originalPayload").GetString().Should().Be("invalid json");
    }

    private static ConsumeResult<string, string> CreateConsumeResult(
    string topic,
    int partition,
    long offset,
    string? key,
    string value)
    {
        var message = new Message<string, string>
        {
            Value = value
        };

        if (key is not null)
        {
            message.Key = key;
        }

        return new ConsumeResult<string, string>
        {
            Topic = topic,
            Partition = new Partition(partition),
            Offset = new Offset(offset),
            Message = message
        };
    }
}